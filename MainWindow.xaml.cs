using server;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Management;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Runtime;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Interop;
using System.Windows.Media;
using System.Windows.Media.Effects;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;
using System.Windows.Threading;
using Path = System.IO.Path;

namespace server
{
    public class ContactItem
    {
        public string Hash { get; set; } = "";
        public string DisplayName { get; set; } = "";
        public bool IsOnline { get; set; }
        public bool HasAvatar { get; set; }
        public long AvatarTs { get; set; }
        public BitmapImage? AvatarSource { get; set; }
        public int UnreadCount { get; set; } = 0;
        public bool HasUnread => UnreadCount > 0;
        public string UnreadText => UnreadCount > 9 ? "9+" : UnreadCount.ToString();
        public string Initials => DisplayName.Length >= 2 ? DisplayName[..2].ToUpper() :
                                     Hash.Length >= 7 ? Hash.Substring(4, 3) : "?";
        public string StatusText => IsOnline ? "Online" : "Offline";
        // Статические frozen кисти — не создаём new каждый рендер
        private static readonly Brush _onlineBrush = FreezeBrush(new SolidColorBrush(Color.FromRgb(0x30, 0xFF, 0x80)));
        private static readonly Brush _offlineBrush = FreezeBrush(new SolidColorBrush(Color.FromRgb(0x50, 0x60, 0x70)));
        private static Brush FreezeBrush(SolidColorBrush b) { b.Freeze(); return b; }
        public Brush StatusColor => IsOnline ? _onlineBrush : _offlineBrush;
    }

    public class ContactRequest
    {
        public int Id { get; set; }
        public string FromHash { get; set; } = "";
        public string DisplayName { get; set; } = "";
        public double CreatedAt { get; set; }
    }

    public class GroupItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Owner { get; set; } = "";
        public List<string> Members { get; set; } = new();
        public string MemberCountText => $"{Members.Count} members";
        public int UnreadCount { get; set; } = 0;
        public bool HasUnread => UnreadCount > 0;
        public string UnreadText => UnreadCount > 9 ? "9+" : UnreadCount.ToString();
    }

    public enum MsgStatus { Sending, Delivered, Read }

    public class MessageItem
    {
        public int Id { get; set; }
        public string Sender { get; set; } = "";
        public string Text { get; set; } = "";
        public double Timestamp { get; set; }
        public bool Delivered { get; set; }
        public double ReadAt { get; set; }
        public bool Deleted { get; set; }
        public bool Edited { get; set; }
        public int ReplyToId { get; set; }
        public string ReplyToSender { get; set; } = "";
        public string ReplyToText { get; set; } = "";
        public Dictionary<string, int> Reactions { get; set; } = new();
        public MsgStatus Status =>
            ReadAt > 0 ? MsgStatus.Read :
            Delivered ? MsgStatus.Delivered : MsgStatus.Sending;
    }

    public class AppSettings
    {
        public double WindowScale { get; set; } = 100;
        public string ServerUrl { get; set; } = "";
        public string DisplayName { get; set; } = "";
        public string SavedToken { get; set; } = "";
        public string SavedUserHash { get; set; } = "";
        public string SavedEmail { get; set; } = "";
    }

    public partial class MainWindow : Window
    {
        private static readonly HttpClient _http = CreateHttpClient();
        private static HttpClient CreateHttpClient()
        {
            var handler = new SocketsHttpHandler
            {
                PooledConnectionLifetime = TimeSpan.FromMinutes(5),
                PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
                MaxConnectionsPerServer = 6,
                EnableMultipleHttp2Connections = true,
                AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate,
            };
            return new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(5) };
        }

        private static readonly string SettingsPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bizzard.conf");

        private static readonly string KeyPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "bizzard.key");

        private string? _token, _myHash, _curHash, _myDisplayName;
        private string? _pendingEmail;
        private bool _isCurGroup = false;

        private string BizzardChannelHash = "BZD-BIZZARD000";
        private string BizzardAdminHash = "";
        private bool _isBizzardAdmin = false;
        private bool _isChannel = false;
        private int _curGroupId = 0;

        private ECDiffieHellman _ecdh = null!;
        private readonly Dictionary<string, string> _contactPubKeys = new();
        private readonly List<MessageItem> _lastMessages = new();
        private List<ContactRequest> _pendingRequests = new();

        private AppSettings _settings = new();
        private double _pendingScale = 100;

        private readonly DispatcherTimer _pollTimer = new() { Interval = TimeSpan.FromMilliseconds(500) };
        private readonly DispatcherTimer _statusTimer = new() { Interval = TimeSpan.FromSeconds(15) };
        private readonly DispatcherTimer _heartbeatTimer = new() { Interval = TimeSpan.FromSeconds(6) };

        private const double BASE_W = 1180;
        private const double BASE_H = 760;
        private const string RESOLVER_URL = "http://64.188.90.134/server";
        private string _resolvedUrl = "";

        private static readonly string[] EmojiList = {
            "\U0001F600","\U0001F602","\U0001F60D","\u2764\uFE0F","\U0001F60A","\U0001F44D","\U0001F60E","\U0001F389",
            "\U0001F622","\U0001F621","\U0001F914","\U0001F634","\U0001F973","\U0001F929","\U0001F607","\U0001F644",
            "\U0001F44B","\U0001F64F","\U0001F525","\u2728","\U0001F4AF","\U0001F3AF","\U0001F4AA","\U0001F680",
            "\U0001F605","\U0001F923","\U0001F618","\U0001F970","\U0001F60F","\U0001F612","\U0001F624","\U0001F92F",
            "\U0001F97A","\U0001F62C","\U0001F910","\U0001F636","\U0001F611","\U0001F614","\U0001F61E","\U0001F616",
            "\U0001F62D","\U0001F631","\U0001F628","\U0001F630","\U0001F613","\U0001F917","\U0001F92D","\U0001F92B",
            "\U0001F436","\U0001F431","\U0001F98A","\U0001F43A","\U0001F981","\U0001F42E","\U0001F437","\U0001F438",
            "\U0001F355","\U0001F354","\U0001F35F","\U0001F32E","\U0001F363","\U0001F369","\U0001F382","\u2615",
        };

        private static readonly (string Emoji, string Key)[] ReactionDefs = {
            ("\U0001F44D", "like"),
            ("\u2764\uFE0F", "heart"),
            ("\U0001F602", "laugh"),
            ("\U0001F44E", "dislike"),
        };

        // Кэшированные кисти — создаём один раз, замораживаем
        private static readonly Brush _brushMine = MakeFrozen(new LinearGradientBrush(
            Color.FromRgb(0x29, 0x79, 0xFF), Color.FromRgb(0x00, 0xBB, 0xF0), 45));
        private static readonly Brush _brushOther = MakeFrozen(new SolidColorBrush(Color.FromRgb(0x16, 0x1C, 0x26)));
        private static readonly Brush _brushDeleted = MakeFrozen(new SolidColorBrush(Color.FromRgb(0x1A, 0x1A, 0x2A)));
        private static readonly Brush _brushWhite = Brushes.White;
        private static readonly Brush _brushTimeMe = MakeFrozen(new SolidColorBrush(Color.FromArgb(0xAA, 0xFF, 0xFF, 0xFF)));
        private static readonly Brush _brushTimeOther = MakeFrozen(new SolidColorBrush(Color.FromRgb(0x45, 0x55, 0x65)));
        private static readonly Brush _brushSenderName = MakeFrozen(new SolidColorBrush(Color.FromRgb(0x29, 0x99, 0xFF)));
        private static readonly Brush _brushDeletedText = MakeFrozen(new SolidColorBrush(Color.FromArgb(0x88, 0xFF, 0xFF, 0xFF)));

        private static T MakeFrozen<T>(T b) where T : Freezable { b.Freeze(); return b; }

        public MainWindow()
        {
            InitializeComponent();
            LoadOrCreateKeyPair();
            LoadSettings();
            ApplyScale(_settings.WindowScale);
            _http.DefaultRequestHeaders.UserAgent.ParseAdd("Bizzard-Client/3.1");
            _pollTimer.Tick += async (_, _) => { await PollAll(); };
            _statusTimer.Tick += async (_, _) => await CheckServerStatus();
            _heartbeatTimer.Tick += async (_, _) => await SendHeartbeat();
            this.Closing += (_, _) => { _pollTimer.Stop(); _statusTimer.Stop(); _heartbeatTimer.Stop(); _ecdh?.Dispose(); };
        }

        private void LoadOrCreateKeyPair()
        {
            try
            {
                if (File.Exists(KeyPath))
                {
                    byte[] pkcs8 = Convert.FromBase64String(File.ReadAllText(KeyPath).Trim());
                    _ecdh = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
                    _ecdh.ImportPkcs8PrivateKey(pkcs8, out _);
                    AutoBackupKey();
                }
                else
                {
                    string autoBackup = GetAutoBackupPath();
                    if (File.Exists(autoBackup))
                    {
                        try
                        {
                            byte[] pkcs8 = Convert.FromBase64String(File.ReadAllText(autoBackup).Trim());
                            _ecdh = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
                            _ecdh.ImportPkcs8PrivateKey(pkcs8, out _);
                            File.WriteAllText(KeyPath, File.ReadAllText(autoBackup));
                            System.Diagnostics.Debug.WriteLine("[Crypto] Key restored from auto-backup.");
                            return;
                        }
                        catch { }
                    }
                    _ecdh = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
                    SaveKeyPair();
                    AutoBackupKey();
                }
            }
            catch
            {
                string autoBackup = GetAutoBackupPath();
                if (File.Exists(autoBackup))
                {
                    try
                    {
                        byte[] pkcs8 = Convert.FromBase64String(File.ReadAllText(autoBackup).Trim());
                        _ecdh = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
                        _ecdh.ImportPkcs8PrivateKey(pkcs8, out _);
                        File.WriteAllText(KeyPath, File.ReadAllText(autoBackup));
                        return;
                    }
                    catch { }
                }
                _ecdh = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
                try { File.Delete(KeyPath); } catch { }
                SaveKeyPair();
                AutoBackupKey();
            }
        }

        private static string GetAutoBackupPath()
        {
            string dir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "Bizzard");
            Directory.CreateDirectory(dir);
            return Path.Combine(dir, "bizzard.key");
        }

        private void AutoBackupKey()
        {
            try
            {
                string backup = GetAutoBackupPath();
                File.WriteAllText(backup, File.ReadAllText(KeyPath));
            }
            catch { }
        }

        private void SaveKeyPair()
        {
            try
            {
                byte[] pkcs8 = _ecdh.ExportPkcs8PrivateKey();
                File.WriteAllText(KeyPath, Convert.ToBase64String(pkcs8));
                try
                {
                    var fi = new System.IO.FileInfo(KeyPath);
                    var ac = fi.GetAccessControl();
                    ac.SetAccessRuleProtection(true, false);
                    ac.AddAccessRule(new System.Security.AccessControl.FileSystemAccessRule(
                        System.Security.Principal.WindowsIdentity.GetCurrent().Name,
                        System.Security.AccessControl.FileSystemRights.FullControl,
                        System.Security.AccessControl.AccessControlType.Allow));
                    fi.SetAccessControl(ac);
                }
                catch { }
            }
            catch { }
        }

        private async Task ResolveServerUrl()
        {
            try
            {
                using var hc = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
                var json = await hc.GetStringAsync(RESOLVER_URL);
                var doc = JsonDocument.Parse(json);
                if (doc.RootElement.TryGetProperty("link", out var lnk))
                {
                    var url = lnk.GetString()?.Trim().TrimEnd('/');
                    if (!string.IsNullOrEmpty(url)) _resolvedUrl = url;
                }
            }
            catch { }
        }

        private async void Window_Loaded(object sender, RoutedEventArgs e)
        {
            SizeSlider.Value = _settings.WindowScale;
            SizeLabel.Text = $"{(int)_settings.WindowScale}%";
            _pendingScale = _settings.WindowScale;
            await ResolveServerUrl();
            if (!string.IsNullOrEmpty(_settings.SavedToken))
                _ = TryAutoLogin();
        }

        private async Task TryAutoLogin()
        {
            try
            {
                var req = new HttpRequestMessage(HttpMethod.Post, ServerUrl + "/validate_token");
                req.Headers.Add("token", _settings.SavedToken);
                var res = await _http.SendAsync(req);
                if (!res.IsSuccessStatusCode)
                {
                    _settings.SavedToken = "";
                    _settings.SavedUserHash = "";
                    SaveSettingsFile();
                    return;
                }
                var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                _token = _settings.SavedToken;
                _myHash = doc.RootElement.GetProperty("user_hash").GetString();
                _myDisplayName = doc.RootElement.TryGetProperty("display_name", out var dn)
                                     ? dn.GetString() : _myHash;
                await PostLoginInit();
            }
            catch { }
        }

        private void LoadSettings()
        {
            try { if (File.Exists(SettingsPath)) _settings = JsonSerializer.Deserialize<AppSettings>(File.ReadAllText(SettingsPath)) ?? new AppSettings(); }
            catch { _settings = new AppSettings(); }
        }

        private void SaveSettingsFile()
        { try { File.WriteAllText(SettingsPath, JsonSerializer.Serialize(_settings, new JsonSerializerOptions { WriteIndented = true })); } catch { } }

        private string ServerUrl { get { var u = _settings.ServerUrl?.Trim().TrimEnd('/'); return !string.IsNullOrEmpty(u) ? u : (!string.IsNullOrEmpty(_resolvedUrl) ? _resolvedUrl : RESOLVER_URL); } }

        private void ApplyScale(double pct) { Width = BASE_W * (pct / 100.0); Height = BASE_H * (pct / 100.0); }

        private void AvatarOpenSettings_Click(object sender, RoutedEventArgs e)
            => OpenSettings_Click(sender, e);

        private void OpenSettings_Click(object sender, RoutedEventArgs e)
        {
            _pendingScale = _settings.WindowScale;
            SizeSlider.Value = _settings.WindowScale;
            SizeLabel.Text = $"{(int)_settings.WindowScale}%";
            DisplayNameInput.Text = _myDisplayName ?? _settings.DisplayName ?? "";
            DisplayNameStatus.Visibility = Visibility.Collapsed;

            // Заполняем аватарку и данные в новом settings overlay
            string displayName = string.IsNullOrEmpty(_myDisplayName) ? (_myHash ?? "Me") : _myDisplayName;
            if (SettingsDisplayNameLabel != null) SettingsDisplayNameLabel.Text = displayName;
            if (SettingsHashLabel != null) SettingsHashLabel.Text = _myHash ?? "";
            if (SettingsAvatarInitials != null)
                SettingsAvatarInitials.Text = displayName.Length >= 2 ? displayName[..2].ToUpper() : displayName.ToUpper();

            // Синхронизируем аватарку из кэша
            if (SettingsAvatarImg != null && _avatarCache.TryGetValue(_myHash ?? "", out var bmp) && bmp != null)
            {
                SettingsAvatarImg.Source = bmp;
                SettingsAvatarImg.Visibility = Visibility.Visible;
                if (SettingsAvatarInitials != null) SettingsAvatarInitials.Visibility = Visibility.Collapsed;
            }
            else if (SettingsAvatarImg != null)
            {
                SettingsAvatarImg.Visibility = Visibility.Collapsed;
                if (SettingsAvatarInitials != null) SettingsAvatarInitials.Visibility = Visibility.Visible;
            }

            // Анимация появления
            SettingsOverlay.Opacity = 0;
            SettingsOverlay.Visibility = Visibility.Visible;
            var fadeIn = new System.Windows.Media.Animation.DoubleAnimation(0, 1,
                TimeSpan.FromMilliseconds(200))
            {
                EasingFunction = new System.Windows.Media.Animation.CubicEase
                { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut }
            };
            SettingsOverlay.BeginAnimation(UIElement.OpacityProperty, fadeIn);
        }

        private void SettingsOverlay_BgClick(object sender, MouseButtonEventArgs e)
        {
            if (e.Source == SettingsOverlay) CloseSettings_Click(sender, e);
        }

        private void SettingsCard_Click(object sender, MouseButtonEventArgs e)
            => e.Handled = true;

        private void SettingsHash_Click(object sender, MouseButtonEventArgs e)
        {
            var h = _myHash;
            if (!string.IsNullOrEmpty(h)) { Clipboard.SetText(h); ShowToast("ID copied!"); }
        }

        private void SettingsChangeAvatar_Click(object sender, MouseButtonEventArgs e)
        {
            CloseSettings_Click(sender, new RoutedEventArgs());
            // Имитируем клик по кнопке загрузки аватара
            UploadAvatar_Click(sender, new RoutedEventArgs());
        }

        private void CloseSettings_Click(object sender, RoutedEventArgs e)
        {
            _pendingScale = _settings.WindowScale;
            var fadeOut = new System.Windows.Media.Animation.DoubleAnimation(1, 0,
                TimeSpan.FromMilliseconds(160));
            fadeOut.Completed += (_, _) => SettingsOverlay.Visibility = Visibility.Collapsed;
            SettingsOverlay.BeginAnimation(UIElement.OpacityProperty, fadeOut);
        }

        private void ExportKey_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dlg = new Microsoft.Win32.SaveFileDialog
                {
                    Title = "Export Bizzard Encryption Key",
                    FileName = "bizzard_backup.key",
                    Filter = "Key files (*.key)|*.key|All files (*.*)|*.*",
                    DefaultExt = ".key"
                };
                if (dlg.ShowDialog() != true) return;

                File.Copy(KeyPath, dlg.FileName, overwrite: true);

                ShowToast("Key exported! Store it in a safe place.");
            }
            catch (Exception ex) { ShowToast("Export failed: " + ex.Message, isError: true); }
        }

        private void ImportKey_Click(object sender, RoutedEventArgs e)
        {
            var confirm = MessageBox.Show(
                "⚠  Importing a key will REPLACE your current encryption key.\n\n" +
                "• Messages encrypted with the current key will become unreadable.\n" +
                "• Only import a key if you are restoring from a backup.\n\n" +
                "Are you sure you want to continue?",
                "Import Key — Warning",
                MessageBoxButton.YesNo, MessageBoxImage.Warning);

            if (confirm != MessageBoxResult.Yes) return;

            try
            {
                var dlg = new Microsoft.Win32.OpenFileDialog
                {
                    Title = "Import Bizzard Encryption Key",
                    Filter = "Key files (*.key)|*.key|All files (*.*)|*.*"
                };
                if (dlg.ShowDialog() != true) return;

                string b64 = File.ReadAllText(dlg.FileName).Trim();
                byte[] pkcs8 = Convert.FromBase64String(b64);
                var testKey = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
                testKey.ImportPkcs8PrivateKey(pkcs8, out _);

                File.WriteAllText(KeyPath, b64);
                _ecdh.Dispose();
                _ecdh = testKey;

                _contactPubKeys.Clear();

                _ = UploadPublicKey();

                SettingsOverlay.Visibility = Visibility.Collapsed;
                ShowToast("Encryption key restored successfully!");
            }
            catch (FormatException) { ShowToast("Invalid key file.", isError: true); }
            catch (Exception ex) { ShowToast("Import failed: " + ex.Message, isError: true); }
        }

        private void SizeSlider_PreviewChanged(object sender, RoutedPropertyChangedEventArgs<double> e)
        { if (SizeLabel == null) return; _pendingScale = (int)e.NewValue; SizeLabel.Text = $"{(int)_pendingScale}%"; }

        private void SaveSettings_Click(object sender, RoutedEventArgs e)
        { _settings.WindowScale = _pendingScale; ApplyScale(_settings.WindowScale); SaveSettingsFile(); SettingsOverlay.Visibility = Visibility.Collapsed; }

        private async void SaveDisplayName_Click(object sender, RoutedEventArgs e)
        {
            var name = DisplayNameInput.Text.Trim();
            if (string.IsNullOrEmpty(name) || name.Length > 32)
            { DisplayNameStatus.Text = "Name must be 1-32 characters"; DisplayNameStatus.Foreground = new SolidColorBrush(Color.FromRgb(0xFF, 0x55, 0x55)); DisplayNameStatus.Visibility = Visibility.Visible; return; }
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Post, "/set_display_name", new { display_name = name }));
                if (res.IsSuccessStatusCode)
                {
                    _myDisplayName = name; _settings.DisplayName = name; SaveSettingsFile();
                    MyDisplayNameLabel.Text = name;
                    if (MyAvatarInitialsBlock != null) MyAvatarInitialsBlock.Text = name.Length >= 2 ? name[..2].ToUpper() : name.ToUpper();
                    if (SettingsDisplayNameLabel != null) SettingsDisplayNameLabel.Text = name;
                    if (SettingsAvatarInitials != null) SettingsAvatarInitials.Text = name.Length >= 2 ? name[..2].ToUpper() : name.ToUpper();
                    DisplayNameStatus.Text = "Saved!"; DisplayNameStatus.Foreground = new SolidColorBrush(Color.FromRgb(0x30, 0xFF, 0x80)); DisplayNameStatus.Visibility = Visibility.Visible;
                }
            }
            catch { }
        }

        private void Window_MouseDown(object sender, MouseButtonEventArgs e)
        {
            if (e.ChangedButton == MouseButton.Left && WindowState == WindowState.Normal)
                DragMove();
        }

        private void Window_SizeChanged(object sender, SizeChangedEventArgs e)
        {
        }

        private DispatcherTimer? _toastTimer;

        private void ShowToast(string message, bool isError = false, bool isWarning = false)
        {
            ToastIcon.Text = isError ? "❌" : isWarning ? "⚠️" : "✅";
            ToastText.Text = message;
            ToastBorder.Visibility = Visibility.Visible;

            _toastTimer?.Stop();
            _toastTimer = new DispatcherTimer { Interval = TimeSpan.FromSeconds(3) };
            _toastTimer.Tick += (_, _) =>
            {
                _toastTimer.Stop();
                ToastBorder.Visibility = Visibility.Collapsed;
            };
            _toastTimer.Start();
        }
        private void CloseApp_Click(object sender, RoutedEventArgs e) => Close();
        private void MinimizeApp_Click(object sender, RoutedEventArgs e) => WindowState = WindowState.Minimized;

        private void RestoreWindow_Click(object sender, RoutedEventArgs e)
        {
            if (WindowState == WindowState.Maximized)
            {
                // Возвращаем в дефолт
                WindowState = WindowState.Normal;
                double scale = _settings.WindowScale / 100.0;
                Width = Math.Max(BASE_W * scale, 860);
                Height = Math.Max(BASE_H * scale, 520);
                Left = (SystemParameters.WorkArea.Width - Width) / 2 + SystemParameters.WorkArea.Left;
                Top = (SystemParameters.WorkArea.Height - Height) / 2 + SystemParameters.WorkArea.Top;
            }
            else
            {
                // Разворачиваем на весь экран
                WindowState = WindowState.Maximized;
            }
        }

        private static string GetHwid()
        {
            try { using var s = new ManagementObjectSearcher("SELECT ProcessorId FROM Win32_Processor"); foreach (var o in s.Get()) return o["ProcessorId"]?.ToString() ?? Environment.MachineName; }
            catch { }
            return Environment.MachineName;
        }

        private static bool IsValidEmail(string email) =>
            !string.IsNullOrWhiteSpace(email) && Regex.IsMatch(email.Trim(), @"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$", RegexOptions.IgnoreCase);

        private bool ValidateAuthInputs(bool isRegister)
        {
            bool ok = true;
            if (!IsValidEmail(EmailInput.Text)) { EmailError.Text = "Enter a valid email address"; EmailError.Visibility = Visibility.Visible; ok = false; } else EmailError.Visibility = Visibility.Collapsed;
            int minLen = isRegister ? 4 : 1;
            if (PasswordInput.Password.Length < minLen) { PasswordError.Text = isRegister ? "Password must be at least 4 characters" : "Enter your password"; PasswordError.Visibility = Visibility.Visible; ok = false; } else PasswordError.Visibility = Visibility.Collapsed;
            return ok;
        }

        private HttpRequestMessage Req(HttpMethod method, string path, object? body = null)
        {
            var r = new HttpRequestMessage(method, ServerUrl + path);
            if (_token != null) r.Headers.Add("token", _token);
            if (body != null) r.Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json");
            return r;
        }

        private async Task SendHeartbeat() { try { await _http.SendAsync(Req(HttpMethod.Post, "/heartbeat")); } catch { } }

        private async Task Auth(string path, bool isRegister)
        {
            if (!ValidateAuthInputs(isRegister)) return;
            try
            {
                var payload = new { email = EmailInput.Text.Trim(), password = PasswordInput.Password, hwid = GetHwid() };
                var res = await _http.SendAsync(Req(HttpMethod.Post, path, payload));
                if ((int)res.StatusCode == 429) { ShowToast("Too many requests. Wait 60 seconds.", isWarning: true); return; }
                var body = await res.Content.ReadAsStringAsync();
                if (isRegister)
                {
                    if (res.IsSuccessStatusCode)
                    { _pendingEmail = EmailInput.Text.Trim(); VerifyEmailHint.Text = $"We sent a 6-digit code to {_pendingEmail}. Check your inbox."; VerifyCodeInput.Text = ""; VerifyError.Visibility = Visibility.Collapsed; AuthScreen.Visibility = Visibility.Collapsed; VerifyScreen.Visibility = Visibility.Visible; }
                    else { try { var doc = JsonDocument.Parse(body); EmailError.Text = doc.RootElement.TryGetProperty("detail", out var d) ? d.GetString() ?? "Error" : "Error"; EmailError.Visibility = Visibility.Visible; } catch { MessageBox.Show("Error: " + body); } }
                    return;
                }
                if (!res.IsSuccessStatusCode)
                { try { var doc = JsonDocument.Parse(body); EmailError.Text = doc.RootElement.TryGetProperty("detail", out var d) ? d.GetString() ?? "Error" : "Error"; EmailError.Visibility = Visibility.Visible; } catch { MessageBox.Show("Error: " + body); } return; }
                var resp = JsonDocument.Parse(body);
                _myHash = resp.RootElement.GetProperty("user_hash").GetString();
                _token = resp.RootElement.GetProperty("token").GetString();
                _myDisplayName = resp.RootElement.TryGetProperty("display_name", out var dn) ? dn.GetString() : _myHash;
                _settings.SavedEmail = EmailInput.Text.Trim().ToLower();
                await PostLoginInit();
            }
            catch (Exception ex) { ShowToast("Server error: " + ex.Message, isError: true); }
        }

        private async Task PostLoginInit()
        {
            _settings.SavedToken = _token ?? "";
            _settings.SavedUserHash = _myHash ?? "";
            SaveSettingsFile();

            MyHashDisplay.Text = _myHash; MyDisplayNameLabel.Text = string.IsNullOrEmpty(_myDisplayName) ? _myHash! : _myDisplayName;
            if (MyAvatarInitialsBlock != null) MyAvatarInitialsBlock.Text = (_myDisplayName ?? _myHash ?? "ME").Length >= 2 ? (_myDisplayName ?? _myHash ?? "ME")[..2].ToUpper() : "ME";
            EmailError.Visibility = PasswordError.Visibility = Visibility.Collapsed;
            AuthScreen.Visibility = VerifyScreen.Visibility = Visibility.Collapsed;
            MainScreen.Visibility = Visibility.Visible;
            InputAreaBorder.Visibility = Visibility.Collapsed;
            ChatHeaderBorder.Visibility = Visibility.Collapsed;
            await UploadPublicKey();
            _ = LoadMyAvatar();
            _pollTimer.Start(); _statusTimer.Start(); _heartbeatTimer.Start();
            await SendHeartbeat(); await CheckServerStatus(); await LoadContacts(); await LoadGroups(); await PollContactRequests();
        }

        private async Task UploadPublicKey()
        {
            try { string pub = Convert.ToBase64String(_ecdh.PublicKey.ExportSubjectPublicKeyInfo()); await _http.SendAsync(Req(HttpMethod.Post, "/set_pubkey", new { pubkey = pub })); }
            catch { }
        }

        private async void ConfirmEmail_Click(object sender, RoutedEventArgs e)
        {
            var code = VerifyCodeInput.Text.Trim();
            if (code.Length != 6 || !code.All(char.IsDigit)) { VerifyError.Text = "Enter the 6-digit code from your email"; VerifyError.Visibility = Visibility.Visible; return; }
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Post, "/verify_email", new { email = _pendingEmail, code, hwid = GetHwid() }));
                if ((int)res.StatusCode == 429) { ShowToast("Too many attempts. Wait a minute.", isWarning: true); return; }
                var body = await res.Content.ReadAsStringAsync();
                if (!res.IsSuccessStatusCode) { var doc = JsonDocument.Parse(body); VerifyError.Text = doc.RootElement.TryGetProperty("detail", out var d) ? d.GetString() ?? "Error" : "Wrong code"; VerifyError.Visibility = Visibility.Visible; return; }
                var resp = JsonDocument.Parse(body);
                _myHash = resp.RootElement.GetProperty("user_hash").GetString();
                _token = resp.RootElement.GetProperty("token").GetString();
                _myDisplayName = _myHash;
                _settings.SavedEmail = _pendingEmail?.ToLower() ?? "";
                await PostLoginInit();
            }
            catch (Exception ex) { ShowToast("Server error: " + ex.Message, isError: true); }
        }

        private void BackToLogin_Click(object sender, RoutedEventArgs e) { VerifyScreen.Visibility = Visibility.Collapsed; AuthScreen.Visibility = Visibility.Visible; }
        private async void Login_Click(object sender, RoutedEventArgs e) => await Auth("/login", false);
        private async void Register_Click(object sender, RoutedEventArgs e) => await Auth("/register", true);

        private int _pollTick = 0;

        // Флаг чтобы не запускать новый poll пока предыдущий не завершился
        private bool _pollRunning = false;

        // ─── Перевод сообщений ───────────────────────────────────────────────
        // chatHash -> переводить ли (только для личных чатов)
        private readonly Dictionary<string, bool> _translateActive = new();
        // msgId -> переведённый текст
        private readonly Dictionary<int, string> _translationCache = new();
        private bool IsTranslateActive => !string.IsNullOrEmpty(_curHash) && !_isCurGroup && !_isChannel
            && _translateActive.TryGetValue(_curHash, out var a) && a;

        private async Task PollAll()
        {
            if (_pollRunning) return; // Пропускаем тик если предыдущий ещё идёт
            _pollRunning = true;
            try
            {
                _pollTick++;

                // Группа 1: сообщения текущего чата + параллельные фоновые задачи
                var tasks = new List<Task> { TrySafe(LoadMessages) };

                // 500ms tick: % 2 = каждую секунду, % 4 = каждые 2с, % 6 = каждые 3с
                if (_pollTick % 2 == 0) tasks.Add(TrySafe(PollTyping));
                if (_pollTick % 4 == 0) tasks.Add(TrySafe(LoadContactsLight));
                if (_pollTick % 20 == 0) tasks.Add(TrySafe(LoadGroups));
                if (_pollTick % 10 == 0) tasks.Add(TrySafe(PollContactRequests));
                if (_pollTick % 6 == 0) tasks.Add(TrySafe(PollGroupNotifications));

                await Task.WhenAll(tasks);
            }
            finally { _pollRunning = false; }
        }

        private static async Task TrySafe(Func<Task> fn)
        {
            try { await fn(); }
            catch { }
        }

        private async Task PollContactRequests()
        {
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, "/get_contact_requests"));
                if (!res.IsSuccessStatusCode) return;
                _pendingRequests = JsonDocument.Parse(await res.Content.ReadAsStringAsync())
                    .RootElement.GetProperty("requests").EnumerateArray()
                    .Select(r => new ContactRequest
                    {
                        Id = r.GetProperty("id").GetInt32(),
                        FromHash = r.GetProperty("from_hash").GetString()!,
                        DisplayName = r.TryGetProperty("display_name", out var dn) ? dn.GetString() ?? r.GetProperty("from_hash").GetString()! : r.GetProperty("from_hash").GetString()!,
                        CreatedAt = r.GetProperty("created_at").GetDouble()
                    }).ToList();
                UpdateRequestsBadge();
                if (RequestsOverlay.Visibility == Visibility.Visible) RebuildRequestsPanel();
            }
            catch { }
        }

        private void UpdateRequestsBadge()
        {
            int count = _pendingRequests.Count;
            RequestsBadge.Visibility = count > 0 ? Visibility.Visible : Visibility.Collapsed;
            RequestsBadgeText.Text = count.ToString();
        }

        private void RebuildRequestsPanel()
        {
            RequestsPanel.Children.Clear();
            if (_pendingRequests.Count == 0)
            {
                RequestsPanel.Children.Add(new TextBlock { Text = "No pending requests", Foreground = new SolidColorBrush(Color.FromRgb(0x44, 0x55, 0x66)), FontSize = 13, Margin = new Thickness(14, 16, 14, 16), HorizontalAlignment = HorizontalAlignment.Center });
                return;
            }
            foreach (var req in _pendingRequests)
            {
                var captured = req;
                var card = new Border { Margin = new Thickness(8, 4, 8, 4), Padding = new Thickness(12, 10, 12, 10), CornerRadius = new CornerRadius(12), Background = new SolidColorBrush(Color.FromArgb(0x14, 0x29, 0x79, 0xFF)), BorderBrush = new SolidColorBrush(Color.FromArgb(0x22, 0x29, 0x79, 0xFF)), BorderThickness = new Thickness(1) };
                var grid = new Grid();
                grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
                grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
                var topRow = new Grid { Margin = new Thickness(0, 0, 0, 8) };
                topRow.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
                topRow.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
                var avatar = new Border { Width = 36, Height = 36, CornerRadius = new CornerRadius(18), Margin = new Thickness(0, 0, 10, 0), Background = new LinearGradientBrush(Color.FromRgb(0x1A, 0x30, 0x50), Color.FromRgb(0x29, 0x79, 0xFF), 45) };
                avatar.Child = new TextBlock { Text = req.DisplayName.Length >= 2 ? req.DisplayName[..2].ToUpper() : "?", FontSize = 12, FontWeight = FontWeights.Bold, Foreground = Brushes.White, HorizontalAlignment = HorizontalAlignment.Center, VerticalAlignment = VerticalAlignment.Center };
                Grid.SetColumn(avatar, 0); topRow.Children.Add(avatar);
                var ns = new StackPanel { VerticalAlignment = VerticalAlignment.Center };
                ns.Children.Add(new TextBlock { Text = req.DisplayName, FontSize = 13, FontWeight = FontWeights.SemiBold, Foreground = new SolidColorBrush(Color.FromRgb(0xD8, 0xE4, 0xEE)) });
                ns.Children.Add(new TextBlock { Text = req.FromHash.Length > 18 ? req.FromHash[..18] + "..." : req.FromHash, FontSize = 10, Foreground = new SolidColorBrush(Color.FromRgb(0x44, 0x55, 0x66)) });
                Grid.SetColumn(ns, 1); topRow.Children.Add(ns);
                Grid.SetRow(topRow, 0); grid.Children.Add(topRow);
                var btnRow = new StackPanel { Orientation = Orientation.Horizontal };
                var acceptBtn = new Border { CornerRadius = new CornerRadius(9), Background = new SolidColorBrush(Color.FromArgb(0xFF, 0x1A, 0x55, 0x22)), BorderBrush = new SolidColorBrush(Color.FromArgb(0xAA, 0x30, 0xFF, 0x80)), BorderThickness = new Thickness(1), Padding = new Thickness(14, 6, 14, 6), Margin = new Thickness(0, 0, 8, 0), Cursor = Cursors.Hand };
                acceptBtn.Child = new TextBlock { Text = "  Accept", FontSize = 12, FontWeight = FontWeights.SemiBold, Foreground = new SolidColorBrush(Color.FromRgb(0x30, 0xFF, 0x80)) };
                acceptBtn.MouseDown += async (_, _) => await RespondToRequest(captured.Id, true);
                acceptBtn.MouseEnter += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0xFF, 0x22, 0x77, 0x2A));
                acceptBtn.MouseLeave += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0xFF, 0x1A, 0x55, 0x22));
                var declineBtn = new Border { CornerRadius = new CornerRadius(9), Background = new SolidColorBrush(Color.FromArgb(0xFF, 0x33, 0x14, 0x14)), BorderBrush = new SolidColorBrush(Color.FromArgb(0xAA, 0xFF, 0x44, 0x44)), BorderThickness = new Thickness(1), Padding = new Thickness(14, 6, 14, 6), Cursor = Cursors.Hand };
                declineBtn.Child = new TextBlock { Text = "  Decline", FontSize = 12, FontWeight = FontWeights.SemiBold, Foreground = new SolidColorBrush(Color.FromRgb(0xFF, 0x66, 0x66)) };
                declineBtn.MouseDown += async (_, _) => await RespondToRequest(captured.Id, false);
                declineBtn.MouseEnter += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0xFF, 0x55, 0x1A, 0x1A));
                declineBtn.MouseLeave += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0xFF, 0x33, 0x14, 0x14));
                btnRow.Children.Add(acceptBtn); btnRow.Children.Add(declineBtn);
                Grid.SetRow(btnRow, 1); grid.Children.Add(btnRow);
                card.Child = grid; RequestsPanel.Children.Add(card);
            }
        }

        private async Task RespondToRequest(int requestId, bool accept)
        {
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Post, "/respond_contact_request", new { request_id = requestId, action = accept ? "accept" : "decline" }));
                if (res.IsSuccessStatusCode) { await PollContactRequests(); if (accept) await LoadContacts(); }
            }
            catch { }
        }

        // LoadContactsLight — только online/unread, без тяжёлой логики аватаров
        // Вызывается каждые 3 сека вместо тяжёлого LoadContacts
        private DateTime _lastFullContactsLoad = DateTime.MinValue;

        private async Task LoadContactsLight()
        {
            var contacts = ContactsList.ItemsSource as List<ContactItem>;
            if (contacts == null || contacts.Count == 0)
            {
                // Первый раз — полная загрузка
                await LoadContacts();
                return;
            }

            // Полная загрузка раз в 30 секунд (аватары, имена)
            bool doFull = (DateTime.UtcNow - _lastFullContactsLoad).TotalSeconds > 15;
            if (doFull) { await LoadContacts(); _lastFullContactsLoad = DateTime.UtcNow; return; }

            // Лёгкий запрос — обновляем только online статус и unread
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, "/get_contacts"));
                if (!res.IsSuccessStatusCode) return;
                var arr = JsonDocument.Parse(await res.Content.ReadAsStringAsync())
                    .RootElement.GetProperty("contacts").EnumerateArray().ToList();

                bool anyChanged = false;
                foreach (var item in arr)
                {
                    string hash = item.GetProperty("hash").GetString()!;
                    bool online = item.GetProperty("online").GetBoolean();
                    int serverUnread = item.TryGetProperty("unread_count", out var uc) ? uc.GetInt32() : 0;

                    var ci = contacts.FirstOrDefault(c => c.Hash == hash);
                    if (ci == null) continue;

                    bool isCurrentChat = hash == _curHash && !_isCurGroup;

                    if (ci.IsOnline != online) { ci.IsOnline = online; anyChanged = true; }

                    if (!isCurrentChat && serverUnread > ci.UnreadCount)
                    {
                        ci.UnreadCount = serverUnread;
                        anyChanged = true;
                    }
                }
                if (anyChanged)
                    await Dispatcher.InvokeAsync(() => ContactsList.Items.Refresh(),
                        DispatcherPriority.Background);
            }
            catch { }
        }

        private async Task PollGroupNotifications()
        {
            var groups = GroupsList.ItemsSource as List<GroupItem>;
            if (groups == null || groups.Count == 0) return;
            foreach (var gi in groups)
            {
                // Не опрашиваем текущую открытую группу — LoadMessages уже это делает
                if (_isCurGroup && gi.Id == _curGroupId) continue;
                try
                {
                    var res = await _http.SendAsync(Req(HttpMethod.Get, $"/get_group_messages/{gi.Id}"));
                    if (!res.IsSuccessStatusCode) continue;
                    var msgs = JsonDocument.Parse(await res.Content.ReadAsStringAsync())
                        .RootElement.GetProperty("messages").EnumerateArray()
                        .Select(m => new MessageItem
                        {
                            Id = m.TryGetProperty("id", out var id) ? id.GetInt32() : 0,
                            Sender = m.GetProperty("sender").GetString()!,
                            Text = m.GetProperty("text").GetString()!,
                            Timestamp = m.GetProperty("timestamp").GetDouble(),
                            Deleted = m.TryGetProperty("deleted", out var del) && del.GetBoolean()
                        }).ToList();
                    // TrackUnread сам вызывает Refresh только если нужно
                    TrackUnread(null, gi.Id, msgs);
                }
                catch { }
            }
        }

        private async void ToggleRequests_Click(object sender, RoutedEventArgs e)
        {
            if (RequestsOverlay.Visibility == Visibility.Visible)
            {
                RequestsOverlay.Visibility = Visibility.Collapsed;
                return;
            }

            RequestsOverlay.Visibility = Visibility.Visible;
            if (RequestsOverlay.Child is Border requestsCard)
                PrepareOverlayCard(requestsCard, 0.94, 14, -6);

            AnimateOverlayOpen(RequestsOverlay, RequestsOverlay.Child as Border, 240, 340, 0.94, 14, -6);

            await Dispatcher.Yield(DispatcherPriority.Render);
            RebuildRequestsPanel();
        }

        private void CloseRequests_Click(object sender, RoutedEventArgs e) => RequestsOverlay.Visibility = Visibility.Collapsed;

        private async Task CheckServerStatus()
        {
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, "/"));
                if (!res.IsSuccessStatusCode) return;
                var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                if (doc.RootElement.TryGetProperty("channel_hash", out var ch) && ch.GetString() is string chv && chv.Length > 0)
                    BizzardChannelHash = chv;
                if (doc.RootElement.TryGetProperty("admin_hash", out var ah) && ah.GetString() is string ahv && ahv.Length > 0)
                    BizzardAdminHash = ahv;
                _isBizzardAdmin = !string.IsNullOrEmpty(BizzardAdminHash) &&
                    string.Equals(_myHash?.Trim(), BizzardAdminHash.Trim(), StringComparison.OrdinalIgnoreCase);
            }
            catch { }
        }

        private async Task LoadContacts()
        {
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, "/get_contacts"));
                if (!res.IsSuccessStatusCode) return;
                var rawContacts = JsonDocument.Parse(await res.Content.ReadAsStringAsync())
                    .RootElement.GetProperty("contacts").EnumerateArray().ToList();
                var newList = rawContacts.Select(x => new ContactItem
                {
                    Hash = x.GetProperty("hash").GetString()!,
                    IsOnline = x.GetProperty("online").GetBoolean(),
                    DisplayName = x.TryGetProperty("display_name", out var dn)
                                    ? dn.GetString() ?? x.GetProperty("hash").GetString()!
                                    : x.GetProperty("hash").GetString()!,
                    HasAvatar = x.TryGetProperty("has_avatar", out var ha) && ha.GetBoolean(),
                    AvatarTs = x.TryGetProperty("avatar_ts", out var at2) ? at2.GetInt64() : 0,
                    UnreadCount = x.TryGetProperty("unread_count", out var uc) ? uc.GetInt32() : 0,
                }).ToList();

                var current = (ContactsList.ItemsSource as List<ContactItem>) ?? new();
                bool listChanged = current.Count != newList.Count || current.Any(c => newList.All(n => n.Hash != c.Hash));

                if (listChanged)
                {
                    foreach (var ni in newList)
                    {
                        var existing = current.FirstOrDefault(c => c.Hash == ni.Hash);
                        if (existing?.AvatarSource != null)
                            ni.AvatarSource = existing.AvatarSource;
                        if (existing != null)
                        {
                            // Берём максимум из серверного счётчика и локального
                            // чтобы не терять уведомления накопленные локально
                            ni.UnreadCount = Math.Max(ni.UnreadCount, existing.UnreadCount);
                            // Если чат открыт — обнуляем
                            if (ni.Hash == _curHash && !_isCurGroup)
                                ni.UnreadCount = 0;
                        }
                    }
                    int sel = ContactsList.SelectedIndex;
                    ContactsList.ItemsSource = null;
                    ContactsList.ItemsSource = newList;
                    if (sel >= 0 && sel < newList.Count) ContactsList.SelectedIndex = sel;

                    foreach (var ci in newList.Where(c => c.HasAvatar))
                    {
                        var cap = ci;
                        bool stale = cap.AvatarTs > 0 && _avatarTsCache.TryGetValue(cap.Hash, out var cts) && cts != cap.AvatarTs;
                        if (cap.AvatarSource == null || stale) _ = LoadAndSetAvatar(cap, stale || cap.AvatarSource == null);
                    }
                }
                else
                {
                    bool anyChanged = false;
                    for (int i = 0; i < current.Count; i++)
                    {
                        var cur = current[i];
                        // Находим по хэшу, а не по индексу — список мог переупорядочиться
                        var nw = newList.FirstOrDefault(n => n.Hash == cur.Hash);
                        if (nw == null) continue;

                        bool changed = cur.IsOnline != nw.IsOnline
                                    || cur.DisplayName != nw.DisplayName
                                    || cur.HasAvatar != nw.HasAvatar
                                    || cur.AvatarTs != nw.AvatarTs;

                        if (changed)
                        {
                            cur.IsOnline = nw.IsOnline;
                            cur.DisplayName = nw.DisplayName;
                            bool avatarJustAdded = !cur.HasAvatar && nw.HasAvatar;
                            cur.HasAvatar = nw.HasAvatar;
                            cur.AvatarTs = nw.AvatarTs;
                            anyChanged = true;
                            if (avatarJustAdded || (cur.HasAvatar && cur.AvatarSource == null))
                            {
                                var cap = cur;
                                _ = LoadAndSetAvatar(cap, true);
                            }
                        }
                        else if (cur.HasAvatar)
                        {
                            bool stale = nw.AvatarTs > 0 && _avatarTsCache.TryGetValue(cur.Hash, out var cts) && cts != nw.AvatarTs;
                            if (stale || cur.AvatarSource == null)
                            {
                                var cap = cur;
                                cap.AvatarTs = nw.AvatarTs;
                                _ = LoadAndSetAvatar(cap, stale || cur.AvatarSource == null);
                            }
                        }

                        // Обновляем unread из сервера если чат не открыт
                        if (cur.Hash != _curHash || _isCurGroup)
                        {
                            int serverUnread = nw.UnreadCount;
                            if (serverUnread > cur.UnreadCount)
                            {
                                cur.UnreadCount = serverUnread;
                                anyChanged = true;
                            }
                        }
                    }
                    if (anyChanged)
                        _ = Dispatcher.InvokeAsync(() => ContactsList.Items.Refresh(), DispatcherPriority.Background);
                }
                if (!string.IsNullOrEmpty(_curHash) && !_isCurGroup)
                {
                    var open = newList.FirstOrDefault(c => c.Hash == _curHash);
                    if (open != null)
                    {
                        if (CurrentChatTitle.Text != open.DisplayName)
                            CurrentChatTitle.Text = open.DisplayName;
                        CurrentChatStatus.Text = open.IsOnline ? "Online" : "Last seen recently";
                        ChatStatusDot.Fill = new SolidColorBrush(open.IsOnline
                            ? Color.FromRgb(0x30, 0xFF, 0x80) : Color.FromRgb(0x50, 0x60, 0x70));
                    }
                }
            }
            catch { }
        }

        private async Task LoadGroups()
        {
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, "/get_groups"));
                if (!res.IsSuccessStatusCode) return;
                GroupsList.ItemsSource = JsonDocument.Parse(await res.Content.ReadAsStringAsync()).RootElement.GetProperty("groups").EnumerateArray()
                    .Select(g => new GroupItem { Id = g.GetProperty("id").GetInt32(), Name = g.GetProperty("name").GetString()!, Owner = g.GetProperty("owner").GetString()!, Members = g.GetProperty("members").EnumerateArray().Select(m => m.GetString()!).ToList() }).ToList();
            }
            catch { }
        }

        private async void AddContact_Click(object sender, RoutedEventArgs e)
        {
            if (_isCurGroup)
            {
                RefreshGroupContactPicker();
                CreateGroupOverlay.Visibility = Visibility.Visible;
                return;
            }
            var hash = NewContactHashInput.Text.Trim();
            if (string.IsNullOrEmpty(hash)) return;
            var res = await _http.SendAsync(Req(HttpMethod.Post, "/send_contact_request", new { contact_hash = hash }));
            if (res.IsSuccessStatusCode)
            {
                var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                string msg = doc.RootElement.TryGetProperty("message", out var m) ? m.GetString() ?? "" : "";
                NewContactHashInput.Clear();
                if (msg.Contains("contacts") || msg.Contains("accepted")) await LoadContacts();
                else ShowToast("Contact request sent!");
            }
            else { try { var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync()); ShowToast(doc.RootElement.TryGetProperty("detail", out var d) ? d.GetString()! : "Error", isError: true); } catch { ShowToast("Error sending request", isError: true); } }
        }

        private void TabChats_Click(object sender, RoutedEventArgs e)
        {
            _isCurGroup = false;
            _curGroupId = 0;
            GroupsList.SelectedIndex = -1;

            if (ChatHeaderBorder.Visibility == Visibility.Visible && _curHash == null)
            {
                _lastMessages.Clear();
                MessagesPanel.Children.Clear();
                ChatHeaderBorder.Visibility = Visibility.Collapsed;
                InputAreaBorder.Visibility = Visibility.Collapsed;
            }
            ContactsList.Visibility = Visibility.Visible;
            GroupsList.Visibility = Visibility.Collapsed;
            AddContactRow.Visibility = Visibility.Visible;
            AddGroupRow.Visibility = Visibility.Collapsed;
            TabChatsBtn.Style = (Style)FindResource("TabBtnActive");
            TabGroupsBtn.Style = (Style)FindResource("TabBtn");
            CancelReply();
        }
        private void TabGroups_Click(object sender, RoutedEventArgs e)
        {
            _isCurGroup = true;
            _curHash = null;
            _curGroupId = 0;
            ContactsList.SelectedIndex = -1;

            _lastMessages.Clear();
            MessagesPanel.Children.Clear();
            ChatHeaderBorder.Visibility = Visibility.Collapsed;
            InputAreaBorder.Visibility = Visibility.Collapsed;
            ReplyBarBorder.Visibility = Visibility.Collapsed;
            ContactsList.Visibility = Visibility.Collapsed;
            GroupsList.Visibility = Visibility.Visible;
            AddContactRow.Visibility = Visibility.Collapsed;
            AddGroupRow.Visibility = Visibility.Visible;
            TabChatsBtn.Style = (Style)FindResource("TabBtn");
            TabGroupsBtn.Style = (Style)FindResource("TabBtnActive");
            RefreshGroupContactPicker();
            CancelReply();
        }

        private void ContactsList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (ContactsList.SelectedItem is ContactItem ci)
            {
                GroupsList.SelectedIndex = -1;
                _curHash = ci.Hash;
                _isCurGroup = false;
                _curGroupId = 0;
                CurrentChatTitle.Text = ci.DisplayName;
                CurrentChatStatus.Text = ci.IsOnline ? "Online" : "Last seen recently";
                ChatStatusDot.Visibility = Visibility.Visible;
                ChatStatusDot.Fill = ci.IsOnline
                    ? new SolidColorBrush(Color.FromRgb(0x30, 0xFF, 0x80))
                    : new SolidColorBrush(Color.FromRgb(0x50, 0x60, 0x70));

                _isChannel = ci.Hash == BizzardChannelHash;
                ChatHeaderBorder.Visibility = Visibility.Visible;
                InputAreaBorder.Visibility = (_isChannel && !_isBizzardAdmin)
                    ? Visibility.Collapsed : Visibility.Visible;
                // Показываем кнопку перевода только в обычных личных чатах
                if (TranslateBtn != null)
                {
                    TranslateBtn.Visibility = (!_isChannel) ? Visibility.Visible : Visibility.Collapsed;
                    UpdateTranslateButton(IsTranslateActive);
                }

                _lastMessages.Clear();
                MessagesPanel.Children.Clear();
                ClearMessageInput();
                CancelReply();
                ResetTranslate();
                AnimateChatIn();
                ci.UnreadCount = 0;
                ContactsList.Items.Refresh();
                _knownMsgCounts[ci.Hash] = -1;
                _ = LoadMessages();
                if (!_isChannel)
                    _ = _http.SendAsync(Req(HttpMethod.Post, "/mark_read", new { sender_hash = _curHash }));
            }
        }

        private async Task LoadChatHeaderAvatar(ContactItem ci)
        {
            var bmp = await GetOrFetchAvatar(ci.Hash);
            _ = bmp;
        }

        private void AnimateChatIn()
        {
            ChatScroll.Opacity = 0;
            ChatScrollTranslate.Y = 24;

            var ease = new System.Windows.Media.Animation.CubicEase
            { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut };

            var fadeIn = new System.Windows.Media.Animation.DoubleAnimation(0, 1,
                TimeSpan.FromMilliseconds(260))
            { EasingFunction = ease };
            var slideIn = new System.Windows.Media.Animation.DoubleAnimation(24, 0,
                TimeSpan.FromMilliseconds(260))
            { EasingFunction = ease };

            ChatScroll.BeginAnimation(UIElement.OpacityProperty, fadeIn);
            ChatScrollTranslate.BeginAnimation(
                System.Windows.Media.TranslateTransform.YProperty, slideIn);
        }

        // ─── Кнопка "прокрутить вниз" ────────────────────────────────────────────

        private bool _scrollBtnVisible = false;

        private void ChatScroll_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            if (ChatScroll == null || ScrollDownBtn == null) return;

            double max = ChatScroll.ScrollableHeight;
            double pos = ChatScroll.VerticalOffset;
            // Показываем кнопку если прокрутили выше чем на 200px от низа
            bool shouldShow = max > 0 && (max - pos) > 200;

            if (shouldShow != _scrollBtnVisible)
            {
                _scrollBtnVisible = shouldShow;
                var ease = new System.Windows.Media.Animation.CubicEase
                { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut };
                if (shouldShow)
                {
                    ScrollDownBtn.Visibility = Visibility.Visible;
                    var fadeIn = new System.Windows.Media.Animation.DoubleAnimation(0, 1,
                        TimeSpan.FromMilliseconds(200))
                    { EasingFunction = ease };
                    var slideUp = new System.Windows.Media.Animation.DoubleAnimation(10, 0,
                        TimeSpan.FromMilliseconds(200))
                    { EasingFunction = ease };
                    ScrollDownBtn.BeginAnimation(UIElement.OpacityProperty, fadeIn);
                    if (ScrollDownBtn.RenderTransform is ScaleTransform)
                    { /* already has transform */ }
                    ScrollDownBtn.RenderTransform = new TranslateTransform(0, 10);
                    ((TranslateTransform)ScrollDownBtn.RenderTransform)
                        .BeginAnimation(TranslateTransform.YProperty, slideUp);
                }
                else
                {
                    var fadeOut = new System.Windows.Media.Animation.DoubleAnimation(1, 0,
                        TimeSpan.FromMilliseconds(160));
                    fadeOut.Completed += (_, _) =>
                    {
                        if (!_scrollBtnVisible) ScrollDownBtn.Visibility = Visibility.Collapsed;
                    };
                    ScrollDownBtn.BeginAnimation(UIElement.OpacityProperty, fadeOut);
                }
            }
        }

        private void ScrollDownBtn_Click(object sender, MouseButtonEventArgs e)
        {
            if (ChatScroll == null) return;

            double max = ChatScroll.ScrollableHeight;
            double pos = ChatScroll.VerticalOffset;
            double distance = max - pos;

            double ratio = Math.Min(distance / Math.Max(ChatScroll.ActualHeight * 3, 1.0), 1.0);
            int durationMs = (int)(420 + ratio * 520);

            SmoothScrollToBottom(durationMs);
        }

        private bool _smoothScrollActive = false;
        private double _smoothScrollStart;
        private double _smoothScrollTarget;
        private DateTime _smoothScrollStartTime;
        private int _smoothScrollDurationMs;

        private void SmoothScrollToBottom(int durationMs)
        {
            if (ChatScroll == null) return;

            _smoothScrollStart = ChatScroll.VerticalOffset;
            _smoothScrollTarget = ChatScroll.ScrollableHeight;
            _smoothScrollStartTime = DateTime.UtcNow;
            _smoothScrollDurationMs = Math.Max(durationMs, 1);

            if (_smoothScrollActive)
                return;

            _smoothScrollActive = true;
            CompositionTarget.Rendering += SmoothScrollFrame;
        }

        private void SmoothScrollFrame(object? sender, EventArgs e)
        {
            if (ChatScroll == null)
            {
                CompositionTarget.Rendering -= SmoothScrollFrame;
                _smoothScrollActive = false;
                return;
            }

            double elapsed = (DateTime.UtcNow - _smoothScrollStartTime).TotalMilliseconds;
            double t = Math.Min(elapsed / Math.Max(_smoothScrollDurationMs, 1), 1.0);
            double easedT = 0.5 - 0.5 * Math.Cos(Math.PI * t);
            double newOffset = _smoothScrollStart + (_smoothScrollTarget - _smoothScrollStart) * easedT;

            ChatScroll.ScrollToVerticalOffset(newOffset);

            if (t >= 1.0 || Math.Abs(ChatScroll.VerticalOffset - _smoothScrollTarget) < 0.5)
            {
                ChatScroll.ScrollToVerticalOffset(_smoothScrollTarget);
                CompositionTarget.Rendering -= SmoothScrollFrame;
                _smoothScrollActive = false;
            }
        }

        private void GroupsList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (GroupsList.SelectedItem is GroupItem gi)
            {
                ContactsList.SelectedIndex = -1;
                _curHash = null;
                _isCurGroup = true;
                _curGroupId = gi.Id;
                CurrentChatTitle.Text = gi.Name;
                CurrentChatStatus.Text = $"{gi.Members.Count} members";
                ChatStatusDot.Visibility = Visibility.Visible;
                ChatStatusDot.Fill = new SolidColorBrush(Color.FromRgb(0x29, 0x99, 0xFF));
                _isChannel = false;
                ChatHeaderBorder.Visibility = Visibility.Visible;
                InputAreaBorder.Visibility = Visibility.Visible;
                if (TranslateBtn != null)
                    if (TranslateBtn != null) TranslateBtn.Visibility = Visibility.Collapsed;
                _lastMessages.Clear();
                MessagesPanel.Children.Clear();
                CancelReply();
                ResetTranslate();
                AnimateChatIn();
                gi.UnreadCount = 0;
                GroupsList.Items.Refresh();
                _ = LoadMessages();
            }
        }

        private void CopyMyHash_Click(object sender, MouseButtonEventArgs e) { if (!string.IsNullOrEmpty(_myHash)) { Clipboard.SetText(_myHash); ShowToast("ID copied!"); } }

        private void CloseCreateGroup_Click(object sender, RoutedEventArgs e) => CreateGroupOverlay.Visibility = Visibility.Collapsed;

        private void RefreshGroupContactPicker()
        {
            var contacts = ((ContactsList.ItemsSource as List<ContactItem>) ?? new())
                .Where(c => c.Hash != BizzardChannelHash).ToList();
            GroupContactPicker.ItemsSource = contacts;
        }

        private async void CreateGroup_Click(object sender, RoutedEventArgs e)
        {
            var name = GroupNameInput.Text.Trim();
            if (string.IsNullOrEmpty(name))
            {
                CreateGroupError.Text = "Please enter a group name";
                CreateGroupError.Visibility = Visibility.Visible;
                return;
            }

            var members = GroupContactPicker.SelectedItems
                .OfType<ContactItem>()
                .Select(c => c.Hash)
                .ToList();

            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Post, "/create_group",
                    new { name, members }));

                if (res.IsSuccessStatusCode)
                {
                    CreateGroupOverlay.Visibility = Visibility.Collapsed;
                    GroupNameInput.Text = "";
                    GroupContactPicker.SelectedItems.Clear();
                    CreateGroupError.Visibility = Visibility.Collapsed;
                    await LoadGroups();
                }
                else
                {
                    var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                    CreateGroupError.Text = doc.RootElement.TryGetProperty("detail", out var d)
                        ? d.GetString() ?? "Error" : "Error";
                    CreateGroupError.Visibility = Visibility.Visible;
                }
            }
            catch (Exception ex)
            {
                CreateGroupError.Text = "Error: " + ex.Message;
                CreateGroupError.Visibility = Visibility.Visible;
            }
        }

        private async Task LoadMessages()
        {
            if (_isCurGroup && _curGroupId == 0) return;
            if (!_isCurGroup && _curHash == null) return;
            string cacheKey = _isCurGroup ? $"g:{_curGroupId}" : (_isChannel ? "ch" : _curHash!);
            string? snapshotHash = _curHash;
            int snapshotGroupId = _curGroupId;
            bool snapshotGroup = _isCurGroup;
            bool snapshotChannel = _isChannel;
            try
            {
                HttpResponseMessage res = snapshotGroup
                    ? await _http.SendAsync(Req(HttpMethod.Get, $"/get_group_messages/{snapshotGroupId}"))
                    : await _http.SendAsync(Req(HttpMethod.Get, "/get_messages/" + snapshotHash));
                if (!res.IsSuccessStatusCode) return;
                var rawMsgs = JsonDocument.Parse(await res.Content.ReadAsStringAsync()).RootElement.GetProperty("messages").EnumerateArray().ToList();
                // Парсим метаданные синхронно (быстро), собираем список для расшифровки
                var parseList = rawMsgs.Select(m =>
                {
                    string rawText = m.GetProperty("text").GetString()!;
                    string sender = m.GetProperty("sender").GetString()!;
                    bool deleted = m.TryGetProperty("deleted", out var del) && del.GetBoolean();
                    bool edited = m.TryGetProperty("edited", out var ed) && ed.GetBoolean();
                    int replyToId = m.TryGetProperty("reply_to_id", out var rid) ? rid.GetInt32() : 0;
                    string rSender = m.TryGetProperty("reply_sender", out var rs) ? (rs.GetString() ?? "") : "";
                    string rText = m.TryGetProperty("reply_text", out var rt) ? (rt.GetString() ?? "") : "";
                    return new
                    {
                        Id = m.TryGetProperty("id", out var id) ? id.GetInt32() : 0,
                        Sender = sender,
                        RawText = rawText,
                        Timestamp = m.GetProperty("timestamp").GetDouble(),
                        Delivered = m.TryGetProperty("delivered", out var dv) && dv.GetBoolean(),
                        ReadAt = m.TryGetProperty("read_at", out var ra) ? ra.GetDouble() : 0,
                        Deleted = deleted,
                        Edited = edited,
                        ReplyToId = replyToId,
                        ReplySender = rSender,
                        ReplyText = rText,
                        Reactions = m.TryGetProperty("reactions", out var rx) ? ParseReactions(rx) : new Dictionary<string, int>(),
                        NeedsDecrypt = !deleted && !snapshotGroup && !snapshotChannel
                    };
                }).ToList();

                // Предварительно прогреваем кэш производного ключа (один раз)
                if (!snapshotGroup && !snapshotChannel && snapshotHash != null
                    && !_derivedKeyCache.ContainsKey(snapshotHash))
                    await GetDerivedKey(snapshotHash);

                // Расшифровываем параллельно только новые сообщения (которых нет в _lastMessages)
                int alreadyDecrypted = Math.Min(_lastMessages.Count, parseList.Count);
                var toDecrypt = parseList.Skip(alreadyDecrypted).Where(p => p.NeedsDecrypt).ToList();
                var decryptedTexts = new Dictionary<int, string>();

                // После прогрева кэша ключа — расшифровка чисто CPU, быстро
                // Батчи по 30 чтобы не перегружать thread pool
                const int batchSize = 30;
                for (int b = 0; b < toDecrypt.Count; b += batchSize)
                {
                    var batch = toDecrypt.Skip(b).Take(batchSize)
                        .Select(async p => (p.Id, Text: await DecryptMessage(p.RawText, snapshotHash!)))
                        .ToList();
                    foreach (var t in await Task.WhenAll(batch))
                        decryptedTexts[t.Id] = t.Text;
                }

                List<MessageItem> newMsgs = parseList.Select((p, idx) =>
                {
                    string text;
                    if (p.Deleted) text = "Message deleted";
                    else if (!p.NeedsDecrypt) text = p.RawText;
                    else if (idx < alreadyDecrypted && idx < _lastMessages.Count)
                        text = _lastMessages[idx].Text; // уже расшифровано
                    else
                        text = decryptedTexts.TryGetValue(p.Id, out var dt) ? dt : p.RawText;
                    return new MessageItem
                    {
                        Id = p.Id,
                        Sender = p.Sender,
                        Text = text,
                        Timestamp = p.Timestamp,
                        Delivered = p.Delivered,
                        ReadAt = p.ReadAt,
                        Deleted = p.Deleted,
                        Edited = p.Edited,
                        ReplyToId = p.ReplyToId,
                        ReplyToSender = p.ReplySender,
                        ReplyToText = p.ReplyText,
                        Reactions = p.Reactions
                    };
                }).ToList();
                if (snapshotHash != _curHash || snapshotGroupId != _curGroupId) return;
                if (newMsgs.Count == _lastMessages.Count)
                {
                    bool needsTranslateRefresh = false;
                    for (int i = 0; i < newMsgs.Count; i++)
                    {
                        var old = _lastMessages[i];
                        var nw = newMsgs[i];
                        bool changed = old.Status != nw.Status
                                    || old.Text != nw.Text
                                    || old.Deleted != nw.Deleted
                                    || ReactionsDiffer(old.Reactions, nw.Reactions);
                        if (changed)
                        {
                            UpdateBubble(i, nw);
                            _lastMessages[i] = nw;
                            if (nw.Sender != _myHash && !nw.Deleted && !nw.Text.StartsWith("📎FILE:") && !nw.Text.StartsWith("↩"))
                                needsTranslateRefresh = true;
                        }
                    }
                    if (IsTranslateActive)
                    {
                        if (needsTranslateRefresh)
                            _ = TranslateAllVisible();
                        else
                            RefreshReplyPreviewIfNeeded();
                    }
                    return;
                }
                int start = _lastMessages.Count;
                if (start > newMsgs.Count) { start = 0; _lastMessages.Clear(); MessagesPanel.Children.Clear(); }

                // Батчим добавление: отключаем layout во время добавления
                bool needScroll = start < newMsgs.Count;
                if (needScroll)
                {
                    // Проверяем был ли пользователь внизу до добавления сообщений
                    bool wasAtBottom = ChatScroll.ScrollableHeight <= 0 ||
                                       (ChatScroll.ScrollableHeight - ChatScroll.VerticalOffset) < 80;

                    for (int i = start; i < newMsgs.Count; i++) { AddBubble(newMsgs[i]); _lastMessages.Add(newMsgs[i]); }

                    // Скроллим вниз только если пользователь был внизу
                    if (wasAtBottom) ChatScroll.ScrollToBottom();
                }

                RefreshReplyPreviewIfNeeded();
                _msgCache[cacheKey] = new List<MessageItem>(_lastMessages);
                TrackUnread(snapshotGroup ? null : snapshotHash, snapshotGroup ? snapshotGroupId : 0, newMsgs);

                // Если перевод активен — переводим только когда появились новые входящие сообщения
                if (IsTranslateActive)
                {
                    bool needTranslate = newMsgs.Skip(start).Any(m => m.Sender != _myHash
                        && !m.Deleted
                        && !m.Text.StartsWith("📎FILE:")
                        && !m.Text.StartsWith("↩")
                        && !_translationCache.ContainsKey(m.Id));
                    if (needTranslate)
                        _ = TranslateAllVisible();
                    else
                        RefreshReplyPreviewIfNeeded();
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[LoadMessages error] {ex.GetType().Name}: {ex.Message}");
            }
        }

        private static Dictionary<string, int> ParseReactions(JsonElement el)
        {
            var result = new Dictionary<string, int>();
            try
            {
                foreach (var prop in el.EnumerateObject())
                {
                    if (prop.Name == "_users") continue;
                    if (prop.Value.ValueKind == JsonValueKind.Number &&
                        prop.Value.TryGetInt32(out int v) && v > 0)
                        result[prop.Name] = v;
                }
            }
            catch { }
            return result;
        }

        private static bool ReactionsDiffer(Dictionary<string, int> a, Dictionary<string, int> b) { if (a.Count != b.Count) return true; foreach (var kv in a) if (!b.TryGetValue(kv.Key, out int v) || v != kv.Value) return true; return false; }

        private bool _isSendingTyping = false;
        private async void MessageInput_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (_isSendingTyping || IsMessageInputEmpty()) return;
            string? target = _isCurGroup ? $"group:{_curGroupId}" : _curHash; if (target == null) return;
            _isSendingTyping = true; try { await _http.SendAsync(Req(HttpMethod.Post, "/typing", new { target })); } catch { } finally { _isSendingTyping = false; }
        }

        private async Task PollTyping()
        {
            if (_isChannel) return;
            if (_isCurGroup && _curGroupId == 0) return; if (!_isCurGroup && _curHash == null) return;
            string? target = _isCurGroup ? $"group:{_curGroupId}" : _curHash; if (target == null) return;
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, $"/get_typing?target={target}")); if (!res.IsSuccessStatusCode) return;
                var typing = JsonDocument.Parse(await res.Content.ReadAsStringAsync()).RootElement.GetProperty("typing").EnumerateArray().Select(t => t.GetString()!).ToList();
                var typingNames = typing.Select(h => {
                    var contact = (ContactsList.ItemsSource as List<ContactItem>)?.FirstOrDefault(c => c.Hash == h);
                    if (contact != null && !string.IsNullOrEmpty(contact.DisplayName) && contact.DisplayName != h)
                        return contact.DisplayName;
                    return h.Length > 12 ? h[..12] + "…" : h;
                }).ToList();
                TypingIndicatorText.Text = typingNames.Count > 0 ? $"{string.Join(", ", typingNames)} is typing…" : "";
                TypingIndicatorBorder.Visibility = typing.Count > 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch { }
        }

        private async void SendMessage_Click(object sender, RoutedEventArgs e) => await SendMsg();
        private async void MessageInput_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Return && Keyboard.Modifiers == ModifierKeys.None)
            {
                e.Handled = true;
                await SendMsg();
            }
            else if (e.Key == Key.Return && Keyboard.Modifiers == ModifierKeys.Shift)
            {
                e.Handled = true;
                var para = MessageInput.Document.Blocks.FirstBlock as Paragraph;
                if (para == null)
                {
                    para = new Paragraph { LineHeight = double.NaN, LineStackingStrategy = LineStackingStrategy.MaxHeight };
                    MessageInput.Document.Blocks.Add(para);
                }
                var lb = new LineBreak();
                var range = new TextRange(MessageInput.CaretPosition, MessageInput.CaretPosition);
                MessageInput.CaretPosition.Paragraph?.Inlines.Add(lb);
                MessageInput.CaretPosition = lb.ElementEnd;
            }
            else if (e.Key == Key.Escape && _emojiOpen)
            {
                EmojiOverlay.Visibility = Visibility.Collapsed;
                _emojiOpen = false;
            }
        }

        private async Task SendMsg()
        {
            var txt = GetMessageText();
            if (string.IsNullOrWhiteSpace(txt)) return;
            int replyId = _replyToId;
            string replySender = _replyToSender;
            string replyText = _replyToText;
            ClearMessageInput();
            CancelReply();
            if (_isCurGroup)
            {
                if (_curGroupId == 0) return;
                string groupTxt = replyId > 0
                    ? "\u21A9" + replySender + "|" + (replyText.Length > 80 ? replyText[..80] : replyText) + "\n" + txt
                    : txt;
                await _http.SendAsync(Req(HttpMethod.Post, "/send_group_message",
                    new { group_id = _curGroupId, text = groupTxt, reply_to_id = replyId, reply_sender = replySender, reply_text = replyText }));
            }
            else
            {
                if (_curHash == null) return;
                if (_isChannel)
                    await _http.SendAsync(Req(HttpMethod.Post, "/send_channel_message", new { text = txt }));
                else
                {
                    string fullText = replyId > 0
                        ? "\u21A9" + replySender + "|" + (replyText.Length > 80 ? replyText[..80] : replyText) + "\n" + txt
                        : txt;
                    string enc = await EncryptMessage(fullText, _curHash);
                    await _http.SendAsync(Req(HttpMethod.Post, "/send_message", new { receiver_hash = _curHash, text = enc, reply_to_id = replyId, reply_sender = replySender, reply_text = replyText }));
                }
            }
            await LoadMessages();
        }

        private static readonly string[] ImageExtensions =
            { ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp" };

        private static string GuessMime(string filename)
        {
            string ext = Path.GetExtension(filename).ToLowerInvariant();
            return ext switch
            {
                ".jpg" or ".jpeg" => "image/jpeg",
                ".png" => "image/png",
                ".gif" => "image/gif",
                ".webp" => "image/webp",
                ".bmp" => "image/bmp",
                ".pdf" => "application/pdf",
                ".zip" => "application/zip",
                ".mp4" => "video/mp4",
                ".mp3" => "audio/mpeg",
                _ => "application/octet-stream"
            };
        }

        private async void AttachBtn_Click(object sender, RoutedEventArgs e)
        {
            if (_curHash == null && !_isCurGroup) return;

            var dlg = new Microsoft.Win32.OpenFileDialog
            {
                Title = "Select file to send (max 1 MB)",
                Filter = "All files (*.*)|*.*"
            };
            if (dlg.ShowDialog() != true) return;

            var fi = new System.IO.FileInfo(dlg.FileName);
            if (fi.Length > 1_048_576)
            {
                ShowToast("File too large. Maximum 1 MB.", isWarning: true);
                return;
            }

            try
            {
                byte[] rawBytes = File.ReadAllBytes(dlg.FileName);
                string filename = fi.Name;
                string mime = GuessMime(filename);
                string datab64 = Convert.ToBase64String(rawBytes);

                var body = new
                {
                    receiver_hash = _isCurGroup ? "" : (_curHash ?? ""),
                    filename,
                    data_b64 = datab64,
                    mime_type = mime,
                    is_group = _isCurGroup ? 1 : 0,
                    group_id = _curGroupId,
                };

                var res = await _http.SendAsync(Req(HttpMethod.Post, "/upload_file", body));
                if (!res.IsSuccessStatusCode)
                {
                    var err = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                    ShowToast(err.RootElement.TryGetProperty("detail", out var d)
                        ? d.GetString()! : "Upload failed", isError: true);
                    return;
                }
                var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                string fid = doc.RootElement.GetProperty("file_id").GetString()!;

                string fileMsg = $"📎FILE:{fid}:{filename}:{fi.Length}:{mime}";
                if (_isCurGroup)
                    await _http.SendAsync(Req(HttpMethod.Post, "/send_group_message",
                        new { group_id = _curGroupId, text = fileMsg, reply_to_id = _replyToId, reply_sender = _replyToSender, reply_text = _replyToText }));
                else
                {
                    string enc = await EncryptMessage(fileMsg, _curHash!);
                    await _http.SendAsync(Req(HttpMethod.Post, "/send_message",
                        new { receiver_hash = _curHash, text = enc, reply_to_id = _replyToId, reply_sender = _replyToSender, reply_text = _replyToText }));
                }
                await LoadMessages();
            }
            catch (Exception ex) { ShowToast("Error sending file: " + ex.Message, isError: true); }
        }

        private async Task DownloadFile(string fileId, string filename, string mimeType,
                                        Action? onStart = null, Action? onDone = null)
        {
            onStart?.Invoke();
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, $"/download_file?file_id={fileId}"));
                if (!res.IsSuccessStatusCode)
                {
                    string detail = "";
                    try
                    {
                        var e = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                        detail = e.RootElement.TryGetProperty("detail", out var d) ? d.GetString()! : "";
                    }
                    catch { }
                    ShowToast(detail == "File expired or deleted" ? "File expired (30 days limit)" : "File not found or access denied.", isError: true);
                    return;
                }
                var doc = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                byte[] data = Convert.FromBase64String(doc.RootElement.GetProperty("data_b64").GetString()!);

                string ext = Path.GetExtension(filename);
                var dlg = new Microsoft.Win32.SaveFileDialog
                {
                    Title = "Save file",
                    FileName = filename,
                    Filter = $"Original (*{ext})|*{ext}|All files (*.*)|*.*",
                    DefaultExt = ext
                };
                if (dlg.ShowDialog() != true) return;
                File.WriteAllBytes(dlg.FileName, data);
                ShowToast("Saved: " + System.IO.Path.GetFileName(dlg.FileName));
            }
            catch (Exception ex) { ShowToast("Download error: " + ex.Message, isError: true); }
            finally { onDone?.Invoke(); }
        }

        private readonly Dictionary<string, BitmapImage?> _avatarCache = new();
        private readonly Dictionary<string, List<MessageItem>> _msgCache = new();
        private readonly Dictionary<string, DateTime> _avatarCacheTime = new();
        private readonly Dictionary<string, long> _avatarTsCache = new();

        private readonly Dictionary<string, int> _knownMsgCounts = new();
        // Кэш загруженных файлов-картинок (file_id -> bitmap)
        private readonly Dictionary<string, BitmapImage?> _fileImageCache = new();
        // Семафор — не более 4 параллельных загрузок картинок
        private readonly System.Threading.SemaphoreSlim _imageLoadSem = new(4, 4);
        private static readonly TimeSpan AvatarCacheTtl = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan MyAvatarCacheTtl = TimeSpan.FromSeconds(5);

        private async Task LoadMyAvatar()
        {
            var bmp = await GetOrFetchAvatar(_myHash ?? "");
            if (bmp != null)
            {
                if (MyAvatarImg != null) MyAvatarImg.Source = bmp;
                if (MyAvatarImg != null) MyAvatarImg.Visibility = Visibility.Visible;
                if (MyAvatarInitialsBlock != null) MyAvatarInitialsBlock.Visibility = Visibility.Collapsed;
            }
        }

        private async Task LoadAndSetAvatar(ContactItem ci, bool force = false)
        {
            var bmp = await GetOrFetchAvatar(ci.Hash, force);
            ci.AvatarSource = bmp;
            if (bmp != null)
            {
                ci.HasAvatar = true;
                _avatarTsCache[ci.Hash] = ci.AvatarTs;
            }
            ContactsList.Items.Refresh();
            if (_curHash == ci.Hash && bmp != null)
            {
            }
        }

        private async Task<BitmapImage?> GetOrFetchAvatar(string userHash, bool force = false)
        {
            var ttl = userHash == _myHash ? MyAvatarCacheTtl : AvatarCacheTtl;
            if (!force && _avatarCache.TryGetValue(userHash, out var cached))
                if (_avatarCacheTime.TryGetValue(userHash, out var t) && DateTime.UtcNow - t < ttl)
                    return cached;
            try
            {
                string avatarUrl = userHash == BizzardChannelHash ? "/get_channel_avatar" : $"/get_avatar/{userHash}";
                // Скачиваем на background thread
                var res = await _http.SendAsync(Req(HttpMethod.Get, avatarUrl)).ConfigureAwait(false);
                if (!res.IsSuccessStatusCode)
                {
                    await Dispatcher.InvokeAsync(() => _avatarCache[userHash] = null, DispatcherPriority.Background);
                    return null;
                }
                var body = await res.Content.ReadAsStringAsync().ConfigureAwait(false);
                byte[] raw = Convert.FromBase64String(
                    JsonDocument.Parse(body).RootElement.GetProperty("data_b64").GetString()!);

                // Декодируем BitmapImage строго на UI thread (WPF требует)
                var bmp = await Dispatcher.InvokeAsync(
                    () => LoadBitmapFromBytes(raw, 256), DispatcherPriority.Background);

                // Обновляем кэш на UI thread (словари не потокобезопасны)
                await Dispatcher.InvokeAsync(() =>
                {
                    _avatarCache[userHash] = bmp;
                    _avatarCacheTime[userHash] = DateTime.UtcNow;
                }, DispatcherPriority.Background);
                return bmp;
            }
            catch
            {
                _avatarCache[userHash] = null;
                return null;
            }
        }

        private static BitmapImage? LoadBitmapFromBytes(byte[] data, int maxPx = 0)
        {
            try
            {
                var ms = new System.IO.MemoryStream(data);
                var bmp = new BitmapImage();
                bmp.BeginInit();
                bmp.StreamSource = ms;
                bmp.CacheOption = BitmapCacheOption.OnLoad;
                bmp.CreateOptions = BitmapCreateOptions.None;
                if (maxPx > 0) bmp.DecodePixelWidth = maxPx;
                bmp.EndInit();
                bmp.Freeze();
                ms.Dispose();
                return bmp;
            }
            catch { return null; }
        }

        private async void UploadAvatar_Click(object sender, RoutedEventArgs e)
        {
            var dlg = new Microsoft.Win32.OpenFileDialog
            {
                Title = "Choose avatar image",
                Filter = "Images (*.png;*.jpg;*.jpeg;*.webp)|*.png;*.jpg;*.jpeg;*.webp"
            };
            if (dlg.ShowDialog() != true) return;
            var fi = new System.IO.FileInfo(dlg.FileName);
            if (fi.Length > 524_288)
            {
                ShowToast("Image too large. Max 500 KB.", isWarning: true);
                return;
            }
            try
            {
                byte[] raw = File.ReadAllBytes(dlg.FileName);
                string b64 = Convert.ToBase64String(raw);
                var res = await _http.SendAsync(Req(HttpMethod.Post, "/set_avatar", new { data_b64 = b64 }));
                if (res.IsSuccessStatusCode)
                {
                    // Инвалидируем кэш немедленно
                    _avatarCache.Remove(_myHash ?? "");
                    _avatarCacheTime.Remove(_myHash ?? "");

                    // Читаем новый avatar_ts из ответа сервера
                    try
                    {
                        var doc2 = JsonDocument.Parse(await res.Content.ReadAsStringAsync());
                        if (doc2.RootElement.TryGetProperty("avatar_ts", out var tsEl))
                            _avatarTsCache[_myHash ?? ""] = tsEl.GetInt64();
                    }
                    catch { _avatarTsCache.Remove(_myHash ?? ""); }

                    var bmp = LoadBitmapFromBytes(raw, 256);
                    if (bmp != null)
                    {
                        _avatarCache[_myHash ?? ""] = bmp;
                        _avatarCacheTime[_myHash ?? ""] = DateTime.UtcNow;
                        if (MyAvatarImg != null) MyAvatarImg.Source = bmp;
                        if (MyAvatarImg != null) MyAvatarImg.Visibility = Visibility.Visible;
                        if (MyAvatarInitialsBlock != null) MyAvatarInitialsBlock.Visibility = Visibility.Collapsed;
                    }
                    ShowToast("Avatar updated!");
                }
            }
            catch (Exception ex) { ShowToast("Error uploading avatar: " + ex.Message, isError: true); }
        }

        private void ShowImagePreview(byte[] imageData, string filename, string fileId, string mimeType)
        {
            var bmp = LoadBitmapFromBytes(imageData);
            if (bmp == null) return;

            ImagePreviewOverlay.Visibility = Visibility.Visible;
            PreviewImage.Source = bmp;
            PreviewFileName.Text = filename;

            PreviewDownloadBtn.Tag = new[] { fileId, filename, mimeType };
        }

        private void PreviewImage_MouseDown(object sender, MouseButtonEventArgs e)
            => e.Handled = true;

        private void ClosePreview_Click(object sender, RoutedEventArgs e)
            => ImagePreviewOverlay.Visibility = Visibility.Collapsed;

        private async void PreviewDownload_Click(object sender, RoutedEventArgs e)
        {
            if (PreviewDownloadBtn.Tag is string[] parts && parts.Length == 3)
                await DownloadFile(parts[0], parts[1], parts[2]);
        }

        private void InsertEmojiIntoInput(string emoji)
        {
            const double sz = 15;

            var img = TwemojiImage(emoji, (int)sz);
            img.Tag = emoji;
            img.Width = sz;
            img.Height = sz;
            img.Margin = new Thickness(1, 0, 1, 0);
            img.VerticalAlignment = VerticalAlignment.Center;

            var para = MessageInput.Document.Blocks.FirstBlock as Paragraph;
            if (para == null)
            {
                para = new Paragraph();
                MessageInput.Document.Blocks.Add(para);
            }
            para.LineHeight = double.NaN;
            para.LineStackingStrategy = LineStackingStrategy.MaxHeight;

            bool isEmpty = !para.Inlines.Any();
            if (isEmpty)
            {
                var anchor = new Run("\u200B")
                {
                    FontSize = 14,
                    Foreground = Brushes.White,
                    BaselineAlignment = BaselineAlignment.Baseline,
                };
                para.Inlines.Add(anchor);
                MessageInput.CaretPosition = anchor.ContentEnd;
            }

            var container = new InlineUIContainer(img, MessageInput.CaretPosition)
            {
                BaselineAlignment = BaselineAlignment.Center
            };
            MessageInput.CaretPosition = container.ElementEnd;
        }

        private string GetMessageText()
        {
            var sb = new System.Text.StringBuilder();
            bool firstBlock = true;
            foreach (var block in MessageInput.Document.Blocks)
            {
                if (!firstBlock) sb.Append("\n");
                firstBlock = false;
                if (block is Paragraph para)
                {
                    foreach (var inline in para.Inlines)
                    {
                        if (inline is LineBreak)
                            sb.Append("\n");
                        else if (inline is Run run)
                            sb.Append(run.Text.Replace("\u200B", ""));
                        else if (inline is InlineUIContainer uic && uic.Child is Image img && img.Tag is string emojiChar)
                            sb.Append(emojiChar);
                    }
                }
            }
            return sb.ToString().Trim();
        }

        private void ClearMessageInput()
        {
            MessageInput.Document.Blocks.Clear();
            var para = new Paragraph
            {
                LineHeight = double.NaN,
                LineStackingStrategy = LineStackingStrategy.MaxHeight,
            };
            MessageInput.Document.Blocks.Add(para);
        }

        private bool IsMessageInputEmpty()
        {
            return string.IsNullOrWhiteSpace(GetMessageText());
        }

        private bool _emojiOpen = false;

        private async void EmojiBtn_Click(object sender, RoutedEventArgs e)
        {
            if (_emojiOpen)
            {
                EmojiOverlay.Visibility = Visibility.Collapsed;
                _emojiOpen = false;
                return;
            }

            EmojiOverlay.Visibility = Visibility.Visible;
            if (EmojiOverlay.Child is Border emojiCard)
                PrepareOverlayCard(emojiCard, 0.92, 16, 0);

            _emojiOpen = true;
            AnimateOverlayOpen(EmojiOverlay, EmojiOverlay.Child as Border, 240, 360, 0.92, 16);

            await Dispatcher.Yield(DispatcherPriority.Render);

            if (EmojiPanel.Children.Count == 0)
            {
                _ = Dispatcher.BeginInvoke(new Action(() =>
                {
                    try
                    {
                        if (EmojiPanel.Children.Count > 0) return;

                        foreach (var emoji in EmojiList)
                        {
                            string cap = emoji;

                            var btn = new Border
                            {
                                Width = 38,
                                Height = 38,
                                Cursor = Cursors.Hand,
                                CornerRadius = new CornerRadius(8),
                                ToolTip = emoji,
                                HorizontalAlignment = HorizontalAlignment.Center,
                                VerticalAlignment = VerticalAlignment.Center,
                                Child = TwemojiImage(emoji, 20),
                            };

                            btn.MouseEnter += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0x28, 0xFF, 0xFF, 0xFF));
                            btn.MouseLeave += (s, _) => ((Border)s).Background = Brushes.Transparent;
                            btn.MouseDown += (_, _) =>
                            {
                                InsertEmojiIntoInput(cap);
                                EmojiOverlay.Visibility = Visibility.Collapsed;
                                _emojiOpen = false;
                                MessageInput.Focus();
                            };
                            EmojiPanel.Children.Add(btn);
                        }
                    }
                    catch
                    {
                    }
                }), DispatcherPriority.Background);
            }
        }

        private static Image TwemojiImage(string emoji, int size)
        {
            try
            {
                var runes = emoji.EnumerateRunes()
                    .Where(r => r.Value != 0xFE0F)
                    .Select(r => r.Value.ToString("x"))
                    .ToList();
                string cp = string.Join("-", runes);
                string url = $"https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72/{cp}.png";

                var bmp = new BitmapImage();
                bmp.BeginInit();
                bmp.UriSource = new Uri(url);
                bmp.CacheOption = BitmapCacheOption.OnLoad;
                bmp.DecodePixelWidth = size * 2;
                bmp.DecodePixelHeight = size * 2;
                bmp.CreateOptions = BitmapCreateOptions.None;
                bmp.EndInit();
                return new Image { Width = size, Height = size, Stretch = Stretch.Uniform, Source = bmp };
            }
            catch
            {
                return new Image { Width = size, Height = size };
            }
        }

        private static TextBlock BuildMessageTextBlock(string text, Brush fg, FontStyle style)
        {
            var tb = new TextBlock { TextWrapping = TextWrapping.Wrap, FontSize = 14, FontStyle = style, Foreground = fg };
            if (string.IsNullOrEmpty(text)) { tb.Inlines.Add(new Run("")); return tb; }

            foreach (var (seg, isEmoji) in SplitTextAndEmoji(text))
            {
                if (isEmoji)
                {
                    var img = TwemojiImage(seg, 16);
                    img.Width = 16; img.Height = 16;
                    img.Margin = new Thickness(1, 0, 1, 0);
                    img.Tag = seg;
                    tb.Inlines.Add(new InlineUIContainer(img) { BaselineAlignment = BaselineAlignment.Center });
                }
                else
                {
                    tb.Inlines.Add(new Run(seg));
                }
            }
            return tb;
        }

        // Кэш для SplitTextAndEmoji — одни и те же сообщения рендерятся при каждом UpdateBubble
        private static readonly Dictionary<string, List<(string text, bool isEmoji)>> _emojiSplitCache = new();

        private static List<(string text, bool isEmoji)> SplitTextAndEmoji(string input)
        {
            if (_emojiSplitCache.TryGetValue(input, out var cached)) return cached;
            var result = new List<(string, bool)>();
            var sb = new System.Text.StringBuilder();
            var en = StringInfo.GetTextElementEnumerator(input);
            while (en.MoveNext())
            {
                string el = en.GetTextElement();
                if (IsEmojiElement(el))
                {
                    if (sb.Length > 0) { result.Add((sb.ToString(), false)); sb.Clear(); }
                    result.Add((el, true));
                }
                else sb.Append(el);
            }
            if (sb.Length > 0) result.Add((sb.ToString(), false));
            // Кэшируем только короткие строки чтобы не раздувать память
            if (input.Length < 500 && _emojiSplitCache.Count < 2000)
                _emojiSplitCache[input] = result;
            return result;
        }

        private static bool IsEmojiElement(string el)
        {
            if (string.IsNullOrEmpty(el)) return false;
            int cp = char.ConvertToUtf32(el, 0);
            return (cp >= 0x1F300 && cp <= 0x1FAFF) ||
                   (cp >= 0x2600 && cp <= 0x27BF) ||
                   (cp >= 0x1F000 && cp <= 0x1F02F) ||
                   (cp >= 0x1F900 && cp <= 0x1F9FF) ||
                    cp == 0x2764;
        }

        private void AddBubble(MessageItem msg)
        {
            bool mine = msg.Sender == _myHash;

            var outer = new StackPanel
            {
                HorizontalAlignment = mine ? HorizontalAlignment.Right : HorizontalAlignment.Left,
                Margin = new Thickness(mine ? 60 : 0, 0, mine ? 0 : 60, 6),
                Tag = msg
            };

            var bubble = new Border
            {
                CornerRadius = mine
                    ? new CornerRadius(18, 18, 4, 18)
                    : new CornerRadius(18, 18, 18, 4),
                Padding = new Thickness(14, 10, 14, 10),
                MaxWidth = 480,
                // DropShadowEffect убран — главный источник лагов при скролле
                Background = msg.Deleted ? _brushDeleted : mine ? _brushMine : _brushOther,
                IsHitTestVisible = !msg.Deleted
            };

            var inner = new StackPanel();

            if (!mine)
            {
                inner.Children.Add(new TextBlock
                {
                    Text = msg.Sender.Length > 12 ? msg.Sender[..12] + "..." : msg.Sender,
                    FontSize = 10,
                    FontWeight = FontWeights.SemiBold,
                    Foreground = _brushSenderName,
                    Margin = new Thickness(0, 0, 0, 4)
                });
            }

            if (!msg.Deleted && msg.Text.StartsWith("📎FILE:"))
            {
                inner.Children.Add(BuildFileBubble(msg));
            }
            else if (!msg.Deleted && msg.Text.StartsWith("\u21A9"))
            {
                var (quoteSender, quotePreview, mainText) = ParseReplyText(msg.Text);
                inner.Children.Add(BuildQuoteBubble(quoteSender, quotePreview, mine, msg.ReplyToId));
                var mainTb = BuildMessageTextBlock(mainText, _brushWhite, FontStyles.Normal);
                mainTb.Tag = $"msg_text:{msg.Id}"; // уникальный тег для поиска при переводе
                inner.Children.Add(mainTb);
            }
            else
            {
                var tb = BuildMessageTextBlock(
                    msg.Text,
                    msg.Deleted ? _brushDeletedText : _brushWhite,
                    msg.Deleted ? FontStyles.Italic : FontStyles.Normal);
                tb.Tag = $"msg_text:{msg.Id}"; // уникальный тег для поиска при переводе
                inner.Children.Add(tb);
            }

            var footer = new Grid();
            footer.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            footer.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });

            var fi = new StackPanel { Orientation = Orientation.Horizontal };
            fi.Children.Add(new TextBlock
            {
                Text = DateTimeOffset.FromUnixTimeSeconds((long)msg.Timestamp).LocalDateTime.ToString("HH:mm"),
                FontSize = 10,
                Foreground = mine ? _brushTimeMe : _brushTimeOther
            });
            Grid.SetColumn(fi, 0);
            footer.Children.Add(fi);

            if (mine && !_isCurGroup)
            {
                var ticks = BuildTicks(msg.Status);
                Grid.SetColumn(ticks, 1);
                footer.Children.Add(ticks);
            }

            inner.Children.Add(footer);

            bubble.PreviewMouseRightButtonDown += (s, e) =>
            {
                if (msg.Deleted) return;
                e.Handled = true;
                Point p = e.GetPosition(RootBorder);
                ShowContextMenu(p, msg.Id, _isChannel, _isCurGroup, mine,
                    GetReplyPreviewForMessage(msg), msg.Sender);
            };

            bubble.Child = inner;
            outer.Children.Add(bubble);
            outer.Children.Add(BuildReactionRow(msg, outer));

            // Плавное появление новых сообщений (только для последних, не при загрузке истории)
            if (MessagesPanel.Children.Count >= _lastMessages.Count && _lastMessages.Count > 0)
            {
                outer.Opacity = 0;
                outer.RenderTransform = new TranslateTransform(0, 12);
                outer.RenderTransformOrigin = new Point(0.5, 0.5);
                var fadeAnim = new System.Windows.Media.Animation.DoubleAnimation(0, 1,
                    TimeSpan.FromMilliseconds(200))
                {
                    EasingFunction = new System.Windows.Media.Animation.CubicEase
                    { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut }
                };
                var slideAnim = new System.Windows.Media.Animation.DoubleAnimation(12, 0,
                    TimeSpan.FromMilliseconds(200))
                {
                    EasingFunction = new System.Windows.Media.Animation.CubicEase
                    { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut }
                };
                outer.BeginAnimation(UIElement.OpacityProperty, fadeAnim);
                ((TranslateTransform)outer.RenderTransform).BeginAnimation(
                    TranslateTransform.YProperty, slideAnim);
            }

            MessagesPanel.Children.Add(outer);
        }

        private FrameworkElement BuildFileBubble(MessageItem msg)
        {
            var raw = msg.Text;
            if (raw.Contains("FILE:")) raw = raw[raw.IndexOf("FILE:")..];
            var parts = raw.Split(':', 5);

            string fileId = parts.Length > 1 ? parts[1] : "";
            string filename = parts.Length > 2 ? parts[2] : "file";
            string sizeStr = parts.Length > 3 ? parts[3] : "0";
            string mime = parts.Length > 4 ? parts[4] : GuessMime(filename);
            long.TryParse(sizeStr, out long filesize);

            string ext = Path.GetExtension(filename).ToLowerInvariant();
            bool isImage = ImageExtensions.Contains(ext) ||
                              mime.StartsWith("image/");
            string sizeText = filesize < 1024 ? $"{filesize} B"
                            : filesize < 1_048_576 ? $"{filesize / 1024.0:F1} KB"
                            : $"{filesize / 1_048_576.0:F1} MB";

            string icon = ext switch
            {
                ".pdf" => "📄",
                ".doc" or ".docx" => "📝",
                ".xls" or ".xlsx" => "📊",
                ".ppt" or ".pptx" => "📋",
                ".zip" or ".rar" or ".7z" => "🗜",
                ".mp4" or ".avi" or ".mov" => "🎬",
                ".mp3" or ".wav" or ".flac" => "🎵",
                ".txt" or ".md" or ".csv" => "📃",
                ".exe" or ".msi" => "⚙",
                ".apk" => "📱",
                _ when isImage => "🖼",
                _ => "📎"
            };

            string captFid = fileId;
            string captName = filename;
            string captMime = mime;

            if (isImage && !string.IsNullOrEmpty(fileId))
            {
                var imgCard = new StackPanel { MinWidth = 220, MaxWidth = 440 };

                var placeholder = new Border
                {
                    Height = 200,
                    CornerRadius = new CornerRadius(12),
                    Background = new SolidColorBrush(Color.FromArgb(0x30, 0xFF, 0xFF, 0xFF)),
                    Margin = new Thickness(0, 0, 0, 6)
                };
                var loadingTb = new TextBlock
                {
                    Text = "🖼  Loading…",
                    FontSize = 13,
                    Foreground = Brushes.White,
                    HorizontalAlignment = HorizontalAlignment.Center,
                    VerticalAlignment = VerticalAlignment.Center
                };
                placeholder.Child = loadingTb;
                imgCard.Children.Add(placeholder);

                imgCard.Children.Add(new TextBlock
                {
                    Text = filename.Length > 28 ? filename[..25] + "…" : filename,
                    FontSize = 11,
                    Foreground = new SolidColorBrush(Color.FromArgb(0xBB, 0xFF, 0xFF, 0xFF))
                });

                // Грузим изображение асинхронно прямо на UI thread — без Task.Run
                _ = LoadImageIntoBubble(captFid, captName, captMime, placeholder, loadingTb);
                return imgCard;
            }

            var container = new Grid { MinWidth = 220 };
            container.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
            container.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            container.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });

            var iconBlock = new Border
            {
                Width = 40,
                Height = 40,
                CornerRadius = new CornerRadius(10),
                Background = new SolidColorBrush(Color.FromArgb(0x30, 0xFF, 0xFF, 0xFF)),
                Margin = new Thickness(0, 0, 10, 0),
                Child = new TextBlock
                {
                    Text = icon,
                    FontSize = 20,
                    HorizontalAlignment = HorizontalAlignment.Center,
                    VerticalAlignment = VerticalAlignment.Center
                }
            };
            Grid.SetColumn(iconBlock, 0); container.Children.Add(iconBlock);

            var nameStack = new StackPanel { VerticalAlignment = VerticalAlignment.Center };
            nameStack.Children.Add(new TextBlock
            {
                Text = filename.Length > 26 ? filename[..23] + "…" + ext : filename,
                FontSize = 13,
                FontWeight = FontWeights.SemiBold,
                Foreground = Brushes.White,
                TextWrapping = TextWrapping.NoWrap,
                TextTrimming = TextTrimming.CharacterEllipsis
            });
            nameStack.Children.Add(new TextBlock
            {
                Text = sizeText,
                FontSize = 11,
                Foreground = new SolidColorBrush(Color.FromArgb(0xBB, 0xFF, 0xFF, 0xFF))
            });
            Grid.SetColumn(nameStack, 1); container.Children.Add(nameStack);

            var dlArea = new Grid { Width = 36, Height = 36, Margin = new Thickness(10, 0, 0, 0) };
            var spin = new System.Windows.Shapes.Ellipse
            {
                Width = 32,
                Height = 32,
                Stroke = new SolidColorBrush(Color.FromArgb(0x66, 0xFF, 0xFF, 0xFF)),
                StrokeThickness = 3,
                Visibility = Visibility.Collapsed
            };
            var dlBtn = new Border
            {
                Width = 36,
                Height = 36,
                CornerRadius = new CornerRadius(18),
                Background = new SolidColorBrush(Color.FromArgb(0x40, 0xFF, 0xFF, 0xFF)),
                Cursor = Cursors.Hand,
                Child = new TextBlock
                {
                    Text = "⬇",
                    FontSize = 16,
                    HorizontalAlignment = HorizontalAlignment.Center,
                    VerticalAlignment = VerticalAlignment.Center,
                    Foreground = Brushes.White
                }
            };
            dlBtn.MouseEnter += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0x60, 0xFF, 0xFF, 0xFF));
            dlBtn.MouseLeave += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0x40, 0xFF, 0xFF, 0xFF));
            dlBtn.MouseDown += async (_, _) =>
            {
                dlBtn.Visibility = Visibility.Collapsed;
                spin.Visibility = Visibility.Visible;
                var rt = new RotateTransform();
                spin.RenderTransformOrigin = new Point(0.5, 0.5);
                spin.RenderTransform = rt;
                var timer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(35) };
                double a = 0;
                timer.Tick += (_, _) => { a = (a + 14) % 360; rt.Angle = a; };
                timer.Start();
                await DownloadFile(captFid, captName, captMime);
                timer.Stop();
                spin.Visibility = Visibility.Collapsed;
                dlBtn.Visibility = Visibility.Visible;
            };
            dlArea.Children.Add(spin);
            dlArea.Children.Add(dlBtn);
            Grid.SetColumn(dlArea, 2); container.Children.Add(dlArea);

            return container;
        }

        private static readonly Brush _ticksRead = MakeFrozen(new SolidColorBrush(Color.FromRgb(0x29, 0x99, 0xFF)));
        private static readonly Brush _ticksPending = MakeFrozen(new SolidColorBrush(Color.FromArgb(0xAA, 0xFF, 0xFF, 0xFF)));

        // ─── Методы перевода ─────────────────────────────────────────────────────

        private void ResetTranslate()
        {
            if (!string.IsNullOrEmpty(_curHash)) _translateActive[_curHash] = false;
            _translationCache.Clear();
            UpdateTranslateButton(false);
            HideTranslateOverlay();
            RefreshReplyPreviewIfNeeded();
        }

        private void TranslateBtn_MouseEnter(object sender, MouseEventArgs e)
        {
            if (sender is not Border btn) return;
            var anim = new System.Windows.Media.Animation.DoubleAnimation(1, 1.04,
                TimeSpan.FromMilliseconds(120))
            {
                EasingFunction = new System.Windows.Media.Animation.CubicEase
                { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut }
            };
            if (btn.RenderTransform is ScaleTransform st)
            {
                st.BeginAnimation(ScaleTransform.ScaleXProperty, anim);
                st.BeginAnimation(ScaleTransform.ScaleYProperty, anim);
            }
        }

        private void TranslateBtn_MouseLeave(object sender, MouseEventArgs e)
        {
            if (sender is not Border btn) return;
            var anim = new System.Windows.Media.Animation.DoubleAnimation(1.04, 1,
                TimeSpan.FromMilliseconds(150))
            {
                EasingFunction = new System.Windows.Media.Animation.CubicEase
                { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut }
            };
            if (btn.RenderTransform is ScaleTransform st)
            {
                st.BeginAnimation(ScaleTransform.ScaleXProperty, anim);
                st.BeginAnimation(ScaleTransform.ScaleYProperty, anim);
            }
        }

        private void TranslateBtn_Click(object sender, MouseButtonEventArgs e)
        {
            ToggleTranslate_Click(sender, e);
        }

        private void ToggleTranslate_Click(object sender, MouseButtonEventArgs e)
        {
            bool wasActive = IsTranslateActive;

            if (wasActive)
            {
                if (!string.IsNullOrEmpty(_curHash)) _translateActive[_curHash] = false;
                _translationCache.Clear();
                UpdateTranslateButton(false);
                RebuildMessagesWithTranslation();
                RefreshReplyPreviewIfNeeded();
            }
            else
            {
                if (!string.IsNullOrEmpty(_curHash)) _translateActive[_curHash] = true;
                _translationCache.Clear();
                UpdateTranslateButton(true);
                _ = TranslateAllVisible();
                RefreshReplyPreviewIfNeeded();
            }
        }

        private void UpdateTranslateButton(bool active)
        {
            if (TranslateBtn == null || TranslateBtnText == null) return;
            if (active)
            {
                TranslateBtnText.Text = "Show Original";
                TranslateBtn.Background = new SolidColorBrush(Color.FromArgb(0x44, 0x33, 0x90, 0xEC));
                TranslateBtn.BorderBrush = new SolidColorBrush(Color.FromRgb(0x33, 0x90, 0xEC));
            }
            else
            {
                TranslateBtnText.Text = "Translate";
                TranslateBtn.Background = new SolidColorBrush(Color.FromArgb(0x1A, 0x33, 0x90, 0xEC));
                TranslateBtn.BorderBrush = new SolidColorBrush(Color.FromArgb(0x72, 0x33, 0x90, 0xEC));
            }
        }



        private static readonly HttpClient _translateHttp = new HttpClient
        { Timeout = TimeSpan.FromSeconds(8) };

        // Переводим все сообщения контакта параллельно батчами + анимация
        private async Task TranslateAllVisible()
        {
            if (!IsTranslateActive) return;

            var toTranslate = _lastMessages
                .Where(m => m.Sender != _myHash          // только сообщения контакта
                    && !m.Deleted                         // не удалённые
                    && !m.Text.StartsWith("📎FILE:")      // не файлы
                    && !m.Text.StartsWith("📎FILE:") // не файлы (alt prefix)
                    && m.Text.Length > 0                  // не пустые
                    && !_translationCache.ContainsKey(m.Id))
                .ToList();

            // Если нечего переводить — просто применяем кэш без анимации
            if (toTranslate.Count == 0) { ApplyTranslationToUI(); return; }

            // Показываем оверлей с анимацией буквы B
            ShowTranslateOverlay(toTranslate.Count);

            int done = 0;
            const int batchSize = 5;
            try
            {
                for (int b = 0; b < toTranslate.Count; b += batchSize)
                {
                    if (!IsTranslateActive) break;
                    var batch = toTranslate.Skip(b).Take(batchSize).ToList();
                    var tasks = batch.Select(m => TranslateOne(m.Id, GetRawText(m))).ToList();
                    await Task.WhenAll(tasks);
                    done += batch.Count;
                    // Обновляем прогресс заполнения буквы B
                    UpdateTranslateProgress(done, toTranslate.Count);
                    // Показываем частичный результат
                    ApplyTranslationToUI();
                }
            }
            finally
            {
                // Скрываем оверлей после завершения (с небольшой задержкой чтобы увидеть 100%)
                await Task.Delay(400);
                HideTranslateOverlay();
            }
        }

        private DispatcherTimer? _glowPulseTimer;

        private void ShowTranslateOverlay(int total)
        {
            if (TranslateOverlay == null) return;
            TranslateOverlay.Visibility = Visibility.Visible;
            TranslateOverlay.Opacity = 0;

            if (TranslateOverlayCount != null)
                TranslateOverlayCount.Text = $"0 / {total}";
            if (TranslateOverlayText != null)
                TranslateOverlayText.Text = "Translating…";

            // Сбрасываем заполнение и прогресс-бар
            if (TranslateFillStop1 != null) TranslateFillStop1.Offset = 0;
            if (TranslateFillStop2 != null) TranslateFillStop2.Offset = 0.001;
            if (TranslateProgressBar != null) TranslateProgressBar.Width = 0;

            // Fade in
            var ease = new System.Windows.Media.Animation.CubicEase
            { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut };
            var fadeIn = new System.Windows.Media.Animation.DoubleAnimation(0, 1,
                TimeSpan.FromMilliseconds(300))
            { EasingFunction = ease };
            TranslateOverlay.BeginAnimation(UIElement.OpacityProperty, fadeIn);

            // Пульсация свечения
            StartGlowPulse();
        }

        private void StartGlowPulse()
        {
            _glowPulseTimer?.Stop();
            if (TranslateGlowInner == null && TranslateGlowOuter == null) return;

            _glowPulseTimer = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(16) };
            double phase = 0;
            _glowPulseTimer.Tick += (_, _) =>
            {
                phase += 0.04;
                double pulse = (Math.Sin(phase) + 1.0) / 2.0; // 0..1

                if (TranslateGlowInner != null)
                    TranslateGlowInner.Opacity = 0.12 + pulse * 0.22;
                if (TranslateGlowOuter != null)
                    TranslateGlowOuter.Opacity = 0.05 + pulse * 0.10;
            };
            _glowPulseTimer.Start();
        }

        private void StopGlowPulse()
        {
            _glowPulseTimer?.Stop();
            _glowPulseTimer = null;
        }

        private void UpdateTranslateProgress(int done, int total)
        {
            if (TranslateOverlay == null) return;
            double ratio = total > 0 ? (double)done / total : 1.0;

            if (TranslateOverlayCount != null)
                TranslateOverlayCount.Text = $"{done} / {total}";

            // Буква B заполняется
            AnimateFillClip(ratio, durationMs: 600);

            // Прогресс-бар
            AnimateProgressBar(ratio);
        }

        private void AnimateFillClip(double ratio, int durationMs = 500)
        {
            // Анимируем заполнение через GradientStop offset
            // ratio=0 → вся буква прозрачная, ratio=1 → вся буква синяя
            if (TranslateFillStop1 == null || TranslateFillStop2 == null) return;

            if (durationMs == 0)
            {
                TranslateFillStop1.Offset = 0;
                TranslateFillStop2.Offset = 0.001;
                return;
            }

            var ease = new System.Windows.Media.Animation.CubicEase
            { EasingMode = System.Windows.Media.Animation.EasingMode.EaseInOut };

            // Stop1 и Stop2 двигаем до ratio — это граница белого/прозрачного
            var anim1 = new System.Windows.Media.Animation.DoubleAnimation(
                TranslateFillStop1.Offset, ratio,
                TimeSpan.FromMilliseconds(durationMs))
            { EasingFunction = ease };
            var anim2 = new System.Windows.Media.Animation.DoubleAnimation(
                TranslateFillStop2.Offset, Math.Min(ratio + 0.001, 1.0),
                TimeSpan.FromMilliseconds(durationMs))
            { EasingFunction = ease };

            TranslateFillStop1.BeginAnimation(GradientStop.OffsetProperty, anim1);
            TranslateFillStop2.BeginAnimation(GradientStop.OffsetProperty, anim2);
        }

        private void AnimateProgressBar(double ratio)
        {
            if (TranslateProgressBar == null) return;
            const double maxW = 180.0;
            double targetW = maxW * ratio;

            var wAnim = new System.Windows.Media.Animation.DoubleAnimation(
                TranslateProgressBar.Width, targetW,
                TimeSpan.FromMilliseconds(500))
            {
                EasingFunction = new System.Windows.Media.Animation.CubicEase
                { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut }
            };
            TranslateProgressBar.BeginAnimation(FrameworkElement.WidthProperty, wAnim);
        }

        private void HideTranslateOverlay()
        {
            StopGlowPulse();
            if (TranslateOverlay == null) return;

            // Анимируем заполнение до 100% перед скрытием
            AnimateFillClip(1.0, durationMs: 300);
            AnimateProgressBar(1.0);

            var ease = new System.Windows.Media.Animation.CubicEase
            { EasingMode = System.Windows.Media.Animation.EasingMode.EaseIn };
            var fadeOut = new System.Windows.Media.Animation.DoubleAnimation(1, 0,
                TimeSpan.FromMilliseconds(350))
            { EasingFunction = ease };
            fadeOut.Completed += (_, _) =>
            {
                if (TranslateOverlay != null)
                    TranslateOverlay.Visibility = Visibility.Collapsed;
            };
            // Небольшая задержка чтобы пользователь увидел 100%
            var delay = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(350) };
            delay.Tick += (_, _) =>
            {
                delay.Stop();
                TranslateOverlay?.BeginAnimation(UIElement.OpacityProperty, fadeOut);
            };
            delay.Start();
        }

        private async Task TranslateOne(int msgId, string text)
        {
            if (string.IsNullOrWhiteSpace(text) || _translationCache.ContainsKey(msgId)) return;
            string result = await TranslateText(text);
            if (!string.IsNullOrEmpty(result) && result != text)
                _translationCache[msgId] = result;
        }

        // LibreTranslate (бесплатный публичный инстанс) + fallback на MyMemory
        private static async Task<string> TranslateText(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) return text;
            // Обрезаем до разумного предела
            string input = text.Length > 500 ? text[..500] : text;
            try
            {
                // MyMemory API — бесплатный, без регистрации, ~5000 слов/день
                // langpair=xx|en где xx — любой язык (auto не работает, используем ru как fallback)
                // На самом деле без langpair тоже работает если убрать auto
                string encoded = Uri.EscapeDataString(input);
                string url = $"https://api.mymemory.translated.net/get?q={encoded}&langpair=ru|en";
                var res = await _translateHttp.GetStringAsync(url).ConfigureAwait(false);
                var doc = JsonDocument.Parse(res);
                // Проверяем статус ответа
                int status = doc.RootElement.TryGetProperty("responseStatus", out var st) ? st.GetInt32() : 0;
                if (status == 200 || status == 0)
                {
                    string? translated = doc.RootElement
                        .GetProperty("responseData")
                        .GetProperty("translatedText")
                        .GetString();
                    if (!string.IsNullOrEmpty(translated) && translated != input
                        && !translated.StartsWith("QUERY LENGTH LIMIT"))
                        return translated;
                }
            }
            catch { }

            // Fallback: Google Translate неофициальный endpoint
            try
            {
                string encoded2 = Uri.EscapeDataString(input);
                string url2 = $"https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=en&dt=t&q={encoded2}";
                var res2 = await _translateHttp.GetStringAsync(url2).ConfigureAwait(false);
                var arr = JsonDocument.Parse(res2).RootElement;
                // Формат: [[["translated","original",...],...],...]
                var sb = new System.Text.StringBuilder();
                foreach (var chunk in arr[0].EnumerateArray())
                {
                    string? part = chunk[0].GetString();
                    if (!string.IsNullOrEmpty(part)) sb.Append(part);
                }
                string result = sb.ToString().Trim();
                if (!string.IsNullOrEmpty(result)) return result;
            }
            catch { }

            return text; // Не смогли — возвращаем оригинал
        }

        // Получаем "чистый" текст для перевода (без prefix цитаты)
        private static string GetRawText(MessageItem msg)
        {
            if (msg.Text.StartsWith("↩"))
            {
                var (_, _, main) = ParseReplyText(msg.Text);
                return main;
            }
            return msg.Text;
        }

        // Применяем переводы к UI без пересоздания пузырей
        private void ApplyTranslationToUI()
        {
            bool active = IsTranslateActive;
            for (int i = 0; i < MessagesPanel.Children.Count && i < _lastMessages.Count; i++)
            {
                if (MessagesPanel.Children[i] is not StackPanel outer) continue;
                var msg = _lastMessages[i];
                if (msg.Sender == _myHash || msg.Deleted || msg.Text.StartsWith("📎FILE:")) continue;

                // Убеждаемся что тег проставлен
                EnsureMsgTextTag(outer, msg.Id);

                // Определяем текст для показа
                string displayText;
                if (active && _translationCache.TryGetValue(msg.Id, out var tr))
                    displayText = tr;
                else
                    displayText = GetRawText(msg);

                UpdateBubbleText(outer, msg.Id, displayText);
            }

            RefreshReplyPreviewIfNeeded();
        }

        // Проставляет тег "msg_text:{id}" на основной TextBlock пузыря если ещё не стоит
        private static void EnsureMsgTextTag(StackPanel outer, int msgId)
        {
            Border? bubble = null;
            for (int i = 0; i < outer.Children.Count; i++)
                if (outer.Children[i] is Border b) { bubble = b; break; }
            if (bubble?.Child is not StackPanel inner) return;

            string targetTag = $"msg_text:{msgId}";
            // Проверяем — может тег уже стоит
            foreach (var child in inner.Children)
                if (child is TextBlock tb && tb.Tag?.ToString() == targetTag) return;

            // Ищем основной TextBlock — последний TextBlock у которого нет Grid-тега
            // (sender name — первый, основной текст — следующий, footer — последний Grid)
            TextBlock? mainTb = null;
            foreach (var child in inner.Children)
            {
                if (child is Grid) break; // дошли до footer
                if (child is TextBlock tb) mainTb = tb; // запоминаем последний TextBlock
            }
            // Если первый TextBlock это sender-name (короткий, SemiBold) — пропускаем его
            // mainTb будет вторым TextBlock (основной текст)
            if (mainTb != null && mainTb.Tag == null)
                mainTb.Tag = targetTag;
        }

        // Обновляет основной TextBlock в пузыре сообщения
        private void UpdateBubbleText(StackPanel outer, int msgId, string newText)
        {
            // Ищем Border (пузырь) — первый дочерний элемент outer
            Border? bubble = null;
            for (int i = 0; i < outer.Children.Count; i++)
            {
                if (outer.Children[i] is Border b) { bubble = b; break; }
            }
            if (bubble?.Child is not StackPanel inner) return;

            string targetTag = $"msg_text:{msgId}";

            // Ищем TextBlock строго по тегу "msg_text:{id}"
            for (int j = 0; j < inner.Children.Count; j++)
            {
                var child = inner.Children[j];
                if (child is TextBlock tb && tb.Tag?.ToString() == targetTag)
                {
                    tb.Inlines.Clear();
                    foreach (var (seg, isEmoji) in SplitTextAndEmoji(newText))
                    {
                        if (isEmoji)
                        {
                            var img = TwemojiImage(seg, 16);
                            img.Width = 16; img.Height = 16; img.Margin = new Thickness(1, 0, 1, 0);
                            tb.Inlines.Add(new InlineUIContainer(img) { BaselineAlignment = BaselineAlignment.Center });
                        }
                        else tb.Inlines.Add(new Run(seg));
                    }
                    return;
                }
            }
        }

        // Устаревший метод — оставляем как алиас для совместимости
        private void RebuildMessagesWithTranslation() => ApplyTranslationToUI();

        // Загружает изображение в плейсхолдер асинхронно — без блокировки UI
        private async Task LoadImageIntoBubble(string fileId, string filename, string mime,
            Border placeholder, TextBlock loadingLabel)
        {
            // Проверяем кэш — если уже загружено, показываем сразу
            if (_fileImageCache.TryGetValue(fileId, out var cachedBmp))
            {
                if (cachedBmp != null) ShowLoadedImage(cachedBmp, fileId, filename, mime, placeholder, null);
                else { loadingLabel.Text = "⏳  File expired"; }
                return;
            }

            // Ограничиваем параллельность — не более 4 одновременных загрузок
            await _imageLoadSem.WaitAsync();
            try
            {
                // Повторная проверка кэша после ожидания семафора
                if (_fileImageCache.TryGetValue(fileId, out var bmp2))
                {
                    if (bmp2 != null) ShowLoadedImage(bmp2, fileId, filename, mime, placeholder, null);
                    else loadingLabel.Text = "⏳  File expired";
                    return;
                }

                // Скачиваем — HTTP запрос не блокирует UI
                var res = await _http.SendAsync(
                    Req(HttpMethod.Get, $"/download_file?file_id={fileId}")).ConfigureAwait(false);

                if (!res.IsSuccessStatusCode)
                {
                    _fileImageCache[fileId] = null;
                    await Dispatcher.InvokeAsync(() => {
                        loadingLabel.Text = "⏳  File expired";
                        loadingLabel.Foreground = new SolidColorBrush(Color.FromArgb(0x88, 0xFF, 0xFF, 0xFF));
                    });
                    return;
                }

                // Читаем и декодируем на background thread
                var body = await res.Content.ReadAsStringAsync().ConfigureAwait(false);
                byte[] data = await Task.Run(() =>
                    Convert.FromBase64String(JsonDocument.Parse(body)
                        .RootElement.GetProperty("data_b64").GetString()!)).ConfigureAwait(false);

                var bmp = await Task.Run(() => LoadBitmapFromBytes(data)).ConfigureAwait(false);

                // Кэшируем результат
                _fileImageCache[fileId] = bmp;

                if (bmp == null)
                {
                    await Dispatcher.InvokeAsync(() => loadingLabel.Text = "⚠  Decode error");
                    return;
                }

                // Обновляем UI на UI thread
                byte[] capData = data;
                string capId = fileId, capName = filename, capMime = mime;
                await Dispatcher.InvokeAsync(() =>
                    ShowLoadedImage(bmp, capId, capName, capMime, placeholder, capData));
            }
            catch
            {
                await Dispatcher.InvokeAsync(() => loadingLabel.Text = "⚠  Load error");
            }
            finally
            {
                _imageLoadSem.Release();
            }
        }

        private void ShowLoadedImage(BitmapImage bmp, string fileId, string filename, string mime,
            Border placeholder, byte[]? data)
        {
            var previewImg = new System.Windows.Controls.Image
            {
                Source = bmp,
                Stretch = Stretch.UniformToFill,
                MaxHeight = 320,
                Cursor = Cursors.Hand
            };
            RenderOptions.SetBitmapScalingMode(previewImg, BitmapScalingMode.HighQuality);
            previewImg.RenderTransformOrigin = new Point(0.5, 0.5);
            previewImg.MouseEnter += (s, _) =>
                ((System.Windows.Controls.Image)s).RenderTransform = new ScaleTransform(1.03, 1.03);
            previewImg.MouseLeave += (s, _) =>
                ((System.Windows.Controls.Image)s).RenderTransform = null;

            if (data != null)
            {
                byte[] capData = data;
                string capId = fileId, capName = filename, capMime = mime;
                previewImg.MouseDown += (_, _) => ShowImagePreview(capData, capName, capId, capMime);
            }
            else
            {
                // Данные уже в кэше — запрашиваем при клике
                string capId = fileId, capName = filename, capMime = mime;
                previewImg.MouseDown += async (_, _) =>
                {
                    if (_fileImageCache.TryGetValue(capId, out var cached) && cached != null)
                    {
                        // Нет сырых данных в кэше — качаем снова только для превью
                        var res2 = await _http.SendAsync(Req(HttpMethod.Get, $"/download_file?file_id={capId}"));
                        if (!res2.IsSuccessStatusCode) return;
                        var b2 = await res2.Content.ReadAsStringAsync();
                        byte[] d2 = Convert.FromBase64String(JsonDocument.Parse(b2).RootElement.GetProperty("data_b64").GetString()!);
                        ShowImagePreview(d2, capName, capId, capMime);
                    }
                };
            }

            var clip = new Border
            {
                CornerRadius = new CornerRadius(10),
                ClipToBounds = true,
                Margin = new Thickness(0, 0, 0, 6),
                Child = previewImg,
                Opacity = 0
            };
            placeholder.Child = clip;
            // Плавное появление
            var fadeIn = new System.Windows.Media.Animation.DoubleAnimation(0, 1,
                TimeSpan.FromMilliseconds(200))
            {
                EasingFunction = new System.Windows.Media.Animation.CubicEase
                { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut }
            };
            clip.BeginAnimation(UIElement.OpacityProperty, fadeIn);
        }

        private static FrameworkElement BuildTicks(MsgStatus status)
        {
            var panel = new StackPanel { Orientation = Orientation.Horizontal, Margin = new Thickness(5, 0, 0, 0), Tag = "ticks" };
            var tickBrush = status == MsgStatus.Read ? _ticksRead : _ticksPending;
            int count = status == MsgStatus.Sending ? 1 : 2;
            for (int i = 0; i < count; i++)
                panel.Children.Add(new TextBlock { Text = "", FontSize = 11, Foreground = tickBrush, Margin = new Thickness(i == 0 ? 0 : -4, 0, 0, 0) });
            return panel;
        }

        private void UpdateBubble(int idx, MessageItem msg)
        {
            if (idx < 0 || idx >= MessagesPanel.Children.Count) return;
            if (MessagesPanel.Children[idx] is not StackPanel outer) return;
            if (outer.Children[0] is not Border bubble) return;
            if (bubble.Child is not StackPanel inner) return;

            bool mine = msg.Sender == _myHash;

            bubble.Background = msg.Deleted ? _brushDeleted : mine ? _brushMine : _brushOther;
            bubble.IsHitTestVisible = !msg.Deleted;
            inner.Children.Clear();

            if (!mine)
                inner.Children.Add(new TextBlock
                {
                    Text = msg.Sender.Length > 12 ? msg.Sender[..12] + "..." : msg.Sender,
                    FontSize = 10,
                    FontWeight = FontWeights.SemiBold,
                    Foreground = _isCurGroup
                        ? new SolidColorBrush(Color.FromRgb(0x29, 0x99, 0xFF))
                        : new SolidColorBrush(Color.FromRgb(0x29, 0x99, 0xFF)),
                    Margin = new Thickness(0, 0, 0, 4)
                });

            if (!msg.Deleted && msg.Text.StartsWith("\U0001F4CEFILE:"))
                inner.Children.Add(BuildFileBubble(msg));
            else if (!msg.Deleted && msg.Text.StartsWith("\u21A9"))
            {
                var (quoteSender, quotePreview, mainText) = ParseReplyText(msg.Text);
                inner.Children.Add(BuildQuoteBubble(quoteSender, quotePreview, mine, msg.ReplyToId));
                inner.Children.Add(BuildMessageTextBlock(mainText, Brushes.White, FontStyles.Normal));
            }
            else
                inner.Children.Add(BuildMessageTextBlock(
                    msg.Text,
                    msg.Deleted
                        ? new SolidColorBrush(Color.FromArgb(0x88, 0xFF, 0xFF, 0xFF))
                        : Brushes.White,
                    msg.Deleted ? FontStyles.Italic : FontStyles.Normal));

            var footer = new Grid { Tag = "footer" };
            footer.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            footer.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
            var fi = new StackPanel { Orientation = Orientation.Horizontal };
            fi.Children.Add(new TextBlock
            {
                Text = DateTimeOffset.FromUnixTimeSeconds((long)msg.Timestamp)
                           .LocalDateTime.ToString("HH:mm"),
                FontSize = 10,
                Foreground = mine
                    ? new SolidColorBrush(Color.FromArgb(0xAA, 0xFF, 0xFF, 0xFF))
                    : new SolidColorBrush(Color.FromRgb(0x45, 0x55, 0x65))
            });
            Grid.SetColumn(fi, 0); footer.Children.Add(fi);
            if (mine && !_isCurGroup)
            {
                var ticks = BuildTicks(msg.Status);
                Grid.SetColumn(ticks, 1);
                footer.Children.Add(ticks);
            }
            inner.Children.Add(footer);

            if (!msg.Deleted)
            {
                if (bubble.Tag?.ToString() == "active")
                    bubble.Tag = "active";
            }
            else
            {
                bubble.Tag = "deleted";
            }

            for (int i = outer.Children.Count - 1; i >= 0; i--)
                if (outer.Children[i] is FrameworkElement fe && fe.Tag?.ToString() == "reaction_row")
                { outer.Children.RemoveAt(i); break; }
            outer.Children.Add(BuildReactionRow(msg, outer));

            // ← FIX реакций: тег на основном TextBlock сброшен — проставляем заново
            // UpdateBubble пересоздаёт inner без тегов, нужно добавить "msg_text:{id}"
            if (!msg.Deleted && bubble.Child is StackPanel freshInner)
            {
                string msgTag = $"msg_text:{msg.Id}";
                // Считаем сколько пропустить: первый TextBlock без тега — либо sender-name (если !mine),
                // либо сразу основной текст (если mine). Ищем ПОСЛЕДНИЙ TextBlock перед footer.
                TextBlock? lastContentTb = null;
                foreach (var child in freshInner.Children)
                {
                    if (child is TextBlock tb && tb.Tag?.ToString() != "footer"
                        && !(tb.Tag is Grid))
                        lastContentTb = tb;
                }
                if (lastContentTb != null && lastContentTb.Tag == null)
                    lastContentTb.Tag = msgTag;

                // Если перевод активен и есть кэш — применяем
                if (!msg.Sender.Equals(_myHash) && IsTranslateActive
                    && _translationCache.TryGetValue(msg.Id, out var cachedTr))
                {
                    UpdateBubbleText(outer, msg.Id, cachedTr);
                }
            }
        }

        private static (string sender, string preview, string mainText) ParseReplyText(string text)
        {
            if (!text.StartsWith("\u21A9")) return ("", "", text);
            var rest = text[1..];
            int pipeIdx = rest.IndexOf('|');
            int nlIdx = rest.IndexOf('\n');
            if (pipeIdx < 0 || nlIdx < 0 || nlIdx <= pipeIdx) return ("", rest, "");
            string sender = rest[..pipeIdx];
            string preview = rest[(pipeIdx + 1)..nlIdx];
            string main = rest[(nlIdx + 1)..];
            return (sender, preview, main);
        }


        private static string NormalizeReplyPreviewText(string text)
        {
            if (string.IsNullOrWhiteSpace(text)) return "";
            if (!text.StartsWith("\u21A9")) return text;
            var (_, _, mainText) = ParseReplyText(text);
            return string.IsNullOrWhiteSpace(mainText) ? text : mainText;
        }

        private string GetVisibleMessageText(MessageItem msg)
        {
            if (msg.Deleted) return msg.Text;
            if (!msg.Sender.Equals(_myHash) && IsTranslateActive
                && _translationCache.TryGetValue(msg.Id, out var tr)
                && !string.IsNullOrWhiteSpace(tr))
            {
                return tr;
            }
            return GetRawText(msg);
        }

        private string GetReplyPreviewForMessage(MessageItem msg)
        {
            return NormalizeReplyPreviewText(GetVisibleMessageText(msg));
        }

        private void RefreshReplyPreviewIfNeeded()
        {
            if (_replyToId <= 0 || ReplyBarBorder == null || ReplyPreviewText == null)
                return;

            var msg = _lastMessages.FirstOrDefault(m => m.Id == _replyToId);
            if (msg == null) return;

            string preview = GetReplyPreviewForMessage(msg);
            if (!string.IsNullOrWhiteSpace(preview))
            {
                _replyToText = preview;
                if (ReplyBarBorder.Visibility == Visibility.Visible)
                    ReplyPreviewText.Text = preview;
            }
        }

        private string ResolveReplyTextForSend()
        {
            if (_replyToId <= 0) return "";
            var msg = _lastMessages.FirstOrDefault(m => m.Id == _replyToId);
            if (msg != null)
            {
                string preview = GetReplyPreviewForMessage(msg);
                if (!string.IsNullOrWhiteSpace(preview))
                    return preview;
            }
            return _replyToText;
        }

        private static FrameworkElement BuildDeletedBlock()
        {
            return new TextBlock
            {
                Text = "Message deleted",
                FontSize = 14,
                FontStyle = FontStyles.Italic,
                Foreground = new SolidColorBrush(Color.FromArgb(0x88, 0xFF, 0xFF, 0xFF)),
                TextWrapping = TextWrapping.Wrap
            };
        }

        private FrameworkElement BuildQuoteBubble(string sender, string preview, bool mine, int messageId)
        {
            var outer = new Border
            {
                CornerRadius = new CornerRadius(10),
                Margin = new Thickness(0, 0, 0, 6),
                Padding = new Thickness(10, 6, 10, 6),
                Cursor = messageId > 0 ? Cursors.Hand : Cursors.Arrow,
                ToolTip = messageId > 0 ? "Open original message" : null,
                Background = new SolidColorBrush(mine
                    ? Color.FromArgb(0x33, 255, 255, 255)
                    : Color.FromArgb(0x22, 51, 144, 236)),
            };

            if (messageId > 0)
            {
                outer.PreviewMouseLeftButtonUp += (_, e) =>
                {
                    e.Handled = true;
                    ScrollToMessage(messageId);
                };
            }

            var grid = new Grid();
            grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
            grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });

            var line = new Border
            {
                Width = 3,
                CornerRadius = new CornerRadius(2),
                Margin = new Thickness(0, 0, 8, 0),
                Background = new SolidColorBrush(mine
                    ? Color.FromRgb(200, 230, 255)
                    : Color.FromRgb(51, 144, 236)),
            };

            Grid.SetColumn(line, 0);
            grid.Children.Add(line);

            var textCol = new StackPanel();

            string senderName =
                string.IsNullOrEmpty(sender) ? "" :
                sender == _myHash ? "You" :
                sender.Length > 18 ? sender[..18] + "..." : sender;

            textCol.Children.Add(new TextBlock
            {
                Text = senderName,
                FontSize = 11,
                FontWeight = FontWeights.SemiBold,
                Foreground = new SolidColorBrush(mine
                    ? Color.FromRgb(220, 240, 255)
                    : Color.FromRgb(120, 180, 255))
            });

            textCol.Children.Add(new TextBlock
            {
                Text = preview,
                FontSize = 12,
                Foreground = new SolidColorBrush(Color.FromArgb(0xCC, 255, 255, 255)),
                TextTrimming = TextTrimming.CharacterEllipsis,
                MaxWidth = 320
            });

            Grid.SetColumn(textCol, 1);
            grid.Children.Add(textCol);

            outer.Child = grid;
            return outer;
        }

        private async void ScrollToMessage(int messageId)
        {
            foreach (StackPanel panel in MessagesPanel.Children)
            {
                if (panel.Tag is MessageItem msg && msg.Id == messageId)
                {
                    panel.BringIntoView();

                    if (panel.Children.Count > 0 && panel.Children[0] is Border border)
                    {
                        var originalBackground = border.Background;
                        var originalEffect = border.Effect;

                        var highlight = new LinearGradientBrush
                        {
                            StartPoint = new Point(0, 0),
                            EndPoint = new Point(1, 1),
                            Opacity = 0.95
                        };
                        highlight.GradientStops.Add(new GradientStop(Color.FromArgb(0xCC, 0x29, 0x79, 0xFF), 0.0));
                        highlight.GradientStops.Add(new GradientStop(Color.FromArgb(0xAA, 0x00, 0xBB, 0xF0), 1.0));

                        border.Background = highlight;
                        border.Effect = new DropShadowEffect
                        {
                            Color = Color.FromRgb(0x29, 0x79, 0xFF),
                            Opacity = 0.65,
                            BlurRadius = 24,
                            ShadowDepth = 0
                        };

                        await Task.Delay(1500);

                        border.Background = originalBackground;
                        border.Effect = originalEffect;
                    }

                    break;
                }
            }
        }


        private FrameworkElement BuildReactionRow(MessageItem msg, StackPanel parentBubble)
        {
            bool isMine = msg.Sender == _myHash;
            var row = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = isMine ? HorizontalAlignment.Right : HorizontalAlignment.Left, Margin = new Thickness(4, 2, 4, 0), Tag = "reaction_row" };
            foreach (var (emoji, key) in ReactionDefs)
            {
                int count = msg.Reactions.TryGetValue(key, out int c) ? c : 0; if (count <= 0) continue;
                var badge = new Border { CornerRadius = new CornerRadius(10), Background = new SolidColorBrush(Color.FromArgb(0x30, 0x29, 0x79, 0xFF)), BorderBrush = new SolidColorBrush(Color.FromArgb(0x55, 0x29, 0x79, 0xFF)), BorderThickness = new Thickness(1), Padding = new Thickness(7, 2, 7, 2), Margin = new Thickness(0, 0, 4, 0), Cursor = Cursors.Hand };
                var bp = new StackPanel { Orientation = Orientation.Horizontal }; bp.Children.Add(TwemojiImage(emoji, 14)); bp.Children.Add(new TextBlock { Text = $" {count}", FontSize = 12, Foreground = Brushes.White, VerticalAlignment = VerticalAlignment.Center }); badge.Child = bp;
                badge.MouseDown += async (_, _) => await ReactToMessage(msg.Id, key, true); row.Children.Add(badge);
            }
            if (!msg.Deleted) { var pb = new Border { Width = 22, Height = 22, CornerRadius = new CornerRadius(11), Background = new SolidColorBrush(Color.FromArgb(0x18, 0xFF, 0xFF, 0xFF)), Cursor = Cursors.Hand, Tag = "plus" }; pb.Child = new TextBlock { Text = "+", FontSize = 14, Foreground = Brushes.White, HorizontalAlignment = HorizontalAlignment.Center }; pb.MouseDown += (_, _) => ToggleReactionPicker(row, msg); row.Children.Add(pb); }
            return row;
        }

        private void ToggleReactionPicker(StackPanel row, MessageItem msg)
        {
            var ex = row.Children.OfType<Border>().FirstOrDefault(b => b.Tag?.ToString() == "picker"); if (ex != null) { row.Children.Remove(ex); return; }
            var picker = new Border { CornerRadius = new CornerRadius(14), Background = new SolidColorBrush(Color.FromRgb(0x1E, 0x26, 0x36)), Padding = new Thickness(6, 4, 6, 4), Margin = new Thickness(6, 0, 0, 0), Tag = "picker" };
            var pp = new StackPanel { Orientation = Orientation.Horizontal };
            foreach (var (emoji, key) in ReactionDefs) { var eb = new Border { Padding = new Thickness(5, 2, 5, 2), Cursor = Cursors.Hand, CornerRadius = new CornerRadius(8) }; eb.Child = TwemojiImage(emoji, 20); eb.MouseDown += async (_, _) => { row.Children.Remove(picker); await ReactToMessage(msg.Id, key); }; eb.MouseEnter += (s, _) => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0x22, 0xFF, 0xFF, 0xFF)); eb.MouseLeave += (s, _) => ((Border)s).Background = Brushes.Transparent; pp.Children.Add(eb); }
            picker.Child = pp; row.Children.Add(picker);
        }

        private void RefreshReactionRow(int msgId)
        {
            int idx = _lastMessages.FindIndex(m => m.Id == msgId);
            if (idx < 0 || idx >= MessagesPanel.Children.Count) return;
            if (MessagesPanel.Children[idx] is not StackPanel outer) return;
            if (_lastMessages[idx] is not MessageItem msg) return;

            for (int i = outer.Children.Count - 1; i >= 0; i--)
            {
                if (outer.Children[i] is FrameworkElement fe && fe.Tag?.ToString() == "reaction_row")
                {
                    outer.Children.RemoveAt(i);
                    break;
                }
            }

            outer.Children.Add(BuildReactionRow(msg, outer));
        }

        private async Task ReactToMessage(int msgId, string reaction, bool isRemoval = false)
        {
            try
            {
                if (_isChannel)
                    await _http.SendAsync(Req(HttpMethod.Post, "/react_channel",
                        new { message_id = msgId, reaction }));
                else if (_isCurGroup)
                    await _http.SendAsync(Req(HttpMethod.Post, "/react_group",
                        new { message_id = msgId, reaction, group_id = _curGroupId }));
                else
                    await _http.SendAsync(Req(HttpMethod.Post, "/react",
                        new { message_id = msgId, reaction, receiver_hash = _curHash }));

                int idx = _lastMessages.FindIndex(m => m.Id == msgId);
                if (idx >= 0 && idx < _lastMessages.Count)
                {
                    var msg = _lastMessages[idx];
                    if (isRemoval)
                    {
                        if (msg.Reactions.TryGetValue(reaction, out int c) && c > 0)
                        {
                            msg.Reactions[reaction] = c - 1;
                            if (msg.Reactions[reaction] <= 0)
                                msg.Reactions.Remove(reaction);
                        }
                    }
                    else
                    {
                        msg.Reactions[reaction] = msg.Reactions.TryGetValue(reaction, out int c) ? c + 1 : 1;
                    }
                    RefreshReactionRow(msgId);
                }
            }
            catch { }
        }

        private int _deleteMsgId = 0;
        private bool _deleteChannel = false;
        private bool _deleteGroup = false;
        private bool _canReply = true;
        private int _replyToId = 0;
        private string _replyToText = "";
        private string _replyToSender = "";

        private void ShowContextMenu(Point mousePos, int msgId, bool isChannel, bool isGroup, bool isMine, string text, string sender)
        {
            _deleteMsgId = msgId;
            _deleteChannel = isChannel;
            _deleteGroup = isGroup;
            _canReply = true;

            // В группе удалять может только автор ИЛИ владелец группы
            bool isGroupOwner = false;
            if (isGroup && _curGroupId > 0)
            {
                var gi = GroupsList.SelectedItem as GroupItem;
                isGroupOwner = gi != null && gi.Owner == _myHash;
            }
            bool showDelete = isMine || (isChannel && _isBizzardAdmin) || (isGroup && (isMine || isGroupOwner));
            DeleteMenuBtn.Visibility = showDelete ? Visibility.Visible : Visibility.Collapsed;
            DeleteMenuDivider.Visibility = (showDelete && _canReply) ? Visibility.Visible : Visibility.Collapsed;
            ReplyMenuBtn.Visibility = Visibility.Visible;

            double menuW = 160;
            double menuH = showDelete ? 88 : 44;
            double x = mousePos.X + 4;
            double y = mousePos.Y - menuH - 4;
            if (y < 8) y = mousePos.Y + 8;
            if (x + menuW > RootBorder.ActualWidth - 8) x = RootBorder.ActualWidth - menuW - 8;
            if (x < 8) x = 8;

            ContextMenuPanel.Margin = new Thickness(x, y, 0, 0);
            DeleteMenuOverlay.Visibility = Visibility.Visible;

            var anim = new System.Windows.Media.Animation.DoubleAnimationUsingKeyFrames();
            anim.KeyFrames.Add(new System.Windows.Media.Animation.EasingDoubleKeyFrame(0.85, System.Windows.Media.Animation.KeyTime.FromTimeSpan(TimeSpan.Zero)));
            anim.KeyFrames.Add(new System.Windows.Media.Animation.EasingDoubleKeyFrame(1.0, System.Windows.Media.Animation.KeyTime.FromTimeSpan(TimeSpan.FromMilliseconds(120)))
            {
                EasingFunction = new System.Windows.Media.Animation.BackEase { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut, Amplitude = 0.25 }
            });
            CtxScale.BeginAnimation(System.Windows.Media.ScaleTransform.ScaleXProperty, anim);
            CtxScale.BeginAnimation(System.Windows.Media.ScaleTransform.ScaleYProperty, anim);

            ReplyMenuBtn.Tag = new object[] { msgId, NormalizeReplyPreviewText(text), sender };
        }

        private void HideDeleteMenu() => DeleteMenuOverlay.Visibility = Visibility.Collapsed;

        private void DeleteOverlayBg_MouseDown(object sender, MouseButtonEventArgs e) => HideDeleteMenu();

        private void DeleteMenuBtn_Enter(object s, MouseEventArgs e)
            => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0x22, 0xFF, 0x55, 0x55));
        private void DeleteMenuBtn_Leave(object s, MouseEventArgs e)
            => ((Border)s).Background = Brushes.Transparent;

        private void ReplyMenuBtn_Enter(object s, MouseEventArgs e)
            => ((Border)s).Background = new SolidColorBrush(Color.FromArgb(0x22, 0x33, 0x90, 0xEC));
        private void ReplyMenuBtn_Leave(object s, MouseEventArgs e)
            => ((Border)s).Background = Brushes.Transparent;

        private void ReplyMenuBtn_Click(object sender, MouseButtonEventArgs e)
        {
            e.Handled = true;
            HideDeleteMenu();
            if (ReplyMenuBtn.Tag is object[] data && data.Length >= 2)
            {
                _replyToId = (int)data[0];
                _replyToText = data[1]?.ToString() ?? "";
                _replyToSender = data.Length > 2 ? data[2]?.ToString() ?? "" : "";
                RefreshReplyPreviewIfNeeded();
                ShowReplyBar(_replyToText);
            }
        }

        private async void DeleteMenuBtn_Click(object sender, MouseButtonEventArgs e)
        {
            e.Handled = true;
            HideDeleteMenu();
            int id = _deleteMsgId;

            MarkMessageDeletedLocally(id);

            if (_deleteChannel) await DeleteChannelMessage(id);
            else if (_deleteGroup) await DeleteGroupMessage(id);
            else await DeleteMessage(id);
        }

        private async Task DeleteMessage(int msgId)
        {
            try
            {
                await _http.SendAsync(Req(HttpMethod.Post, "/delete_message",
                    new { message_id = msgId }));
            }
            catch { }
        }

        private void ShowReplyBar(string previewText)
        {
            string preview = previewText ?? "";
            if (preview.StartsWith("📎FILE:")) preview = "📎 File";
            if (preview.Length > 60) preview = preview[..57] + "…";
            if (ReplyPreviewText != null) ReplyPreviewText.Text = preview;
            if (ReplyBarBorder != null) ReplyBarBorder.Visibility = Visibility.Visible;
        }

        private void CancelReply()
        {
            _replyToId = 0;
            _replyToText = "";
            _replyToSender = "";
            ReplyBarBorder.Visibility = Visibility.Collapsed;
        }

        private void CancelReply_Click(object sender, MouseButtonEventArgs e) => CancelReply();

        private async Task DeleteGroupMessage(int msgId)
        {
            try
            {
                await _http.SendAsync(Req(HttpMethod.Post, "/delete_group_message",
                    new { message_id = msgId }));
            }
            catch { }
        }

        private async Task DeleteChannelMessage(int msgId)
        {
            try
            {
                await _http.SendAsync(Req(HttpMethod.Post, "/delete_channel_message",
                    new { message_id = msgId }));
            }
            catch { }
        }

        private void MarkMessageDeletedLocally(int msgId)
        {
            string cacheKey = _isCurGroup ? $"g:{_curGroupId}" : (_isChannel ? "ch" : _curHash ?? "");
            for (int i = 0; i < _lastMessages.Count; i++)
            {
                if (_lastMessages[i].Id != msgId) continue;

                var msg = _lastMessages[i];
                msg.Deleted = true;
                msg.Text = "Message deleted";
                _lastMessages[i] = msg;
                UpdateBubble(i, msg);

                if (!string.IsNullOrEmpty(cacheKey) && _msgCache.TryGetValue(cacheKey, out var cached))
                {
                    for (int j = 0; j < cached.Count; j++)
                    {
                        if (cached[j].Id != msgId) continue;
                        cached[j].Deleted = true;
                        cached[j].Text = "Message deleted";
                        break;
                    }
                }
                break;
            }
        }

        // Кэш производных AES-ключей — DeriveKeyMaterial дорогая операция
        private readonly Dictionary<string, byte[]> _derivedKeyCache = new();

        private async Task<byte[]?> GetDerivedKey(string peerHash)
        {
            if (_derivedKeyCache.TryGetValue(peerHash, out var cached)) return cached;
            string pub = await GetOrFetchPubKey(peerHash);
            if (string.IsNullOrEmpty(pub)) return null;
            using var peer = ECDiffieHellman.Create();
            peer.ImportSubjectPublicKeyInfo(Convert.FromBase64String(pub), out _);
            var key = _ecdh.DeriveKeyMaterial(peer.PublicKey);
            _derivedKeyCache[peerHash] = key;
            return key;
        }

        private async Task<string> EncryptMessage(string plainText, string peerHash)
        {
            try
            {
                var key = await GetDerivedKey(peerHash);
                if (key == null) return plainText;
                return EncryptAES(plainText, key);
            }
            catch { return plainText; }
        }

        private async Task<string> DecryptMessage(string cipherText, string peerHash)
        {
            try
            {
                var key = await GetDerivedKey(peerHash);
                if (key == null) return cipherText;
                return DecryptAES(cipherText, key);
            }
            catch { return cipherText; }
        }

        private async Task<string> GetOrFetchPubKey(string hash)
        {
            if (_contactPubKeys.TryGetValue(hash, out var k)) return k;
            try
            {
                var res = await _http.SendAsync(Req(HttpMethod.Get, $"/get_pubkey?contact={hash}")); if (!res.IsSuccessStatusCode) return "";
                string pub = JsonDocument.Parse(await res.Content.ReadAsStringAsync()).RootElement.GetProperty("pubkey").GetString() ?? "";
                if (!string.IsNullOrEmpty(pub)) _contactPubKeys[hash] = pub; return pub;
            }
            catch { return ""; }
        }

        private string EncryptAES(string text, byte[] key)
        {
            const int nonceSize = 12; // AesGcm.NonceByteSizes.MaxSize
            const int tagSize = 16;
            // Один буфер вместо Concat аллокаций
            byte[] plain = Encoding.UTF8.GetBytes(text);
            byte[] output = new byte[nonceSize + tagSize + plain.Length];
            RandomNumberGenerator.Fill(output.AsSpan(0, nonceSize));
            using var aes = new AesGcm(key.AsSpan(0, 32), tagSize);
            aes.Encrypt(
                output.AsSpan(0, nonceSize),
                plain,
                output.AsSpan(nonceSize + tagSize),
                output.AsSpan(nonceSize, tagSize));
            return Convert.ToBase64String(output);
        }

        private string DecryptAES(string base64, byte[] key)
        {
            try
            {
                const int nonceSize = 12;
                const int tagSize = 16;
                byte[] data = Convert.FromBase64String(base64);
                if (data.Length < nonceSize + tagSize) return base64;
                int encLen = data.Length - nonceSize - tagSize;
                byte[] plain = new byte[encLen];
                using var aes = new AesGcm(key.AsSpan(0, 32), tagSize);
                aes.Decrypt(
                    data.AsSpan(0, nonceSize),
                    data.AsSpan(nonceSize + tagSize, encLen),
                    data.AsSpan(nonceSize, tagSize),
                    plain);
                return Encoding.UTF8.GetString(plain);
            }
            catch { return base64; }
        }

        private void TrackUnread(string? chatHash, int groupId, List<MessageItem> msgs)
        {
            // Для личных чатов (вызывается только для текущего открытого чата)
            if (!string.IsNullOrEmpty(chatHash))
            {
                int fromContact = msgs.Count(m => m.Sender != _myHash && !m.Deleted);
                // Чат открыт — запоминаем baseline, бейдж уже 0
                if (chatHash == _curHash && !_isCurGroup)
                {
                    _knownMsgCounts[chatHash] = fromContact;
                    return;
                }
                // Чат НЕ открыт
                if (_knownMsgCounts.TryGetValue(chatHash, out int known) && known >= 0)
                {
                    int diff = fromContact - known;
                    if (diff > 0)
                    {
                        var contacts = ContactsList.ItemsSource as List<ContactItem>;
                        var ci = contacts?.FirstOrDefault(c => c.Hash == chatHash);
                        if (ci != null)
                        {
                            ci.UnreadCount = Math.Min(ci.UnreadCount + diff, 99);
                            _ = Dispatcher.InvokeAsync(() => ContactsList.Items.Refresh(), DispatcherPriority.Background);
                        }
                    }
                }
                _knownMsgCounts[chatHash] = fromContact;
                return;
            }

            // Для групп
            if (groupId > 0)
            {
                string gKey = $"g:{groupId}";
                int fromOthers = msgs.Count(m => m.Sender != _myHash && !m.Deleted);
                if (_isCurGroup && groupId == _curGroupId)
                {
                    _knownMsgCounts[gKey] = fromOthers;
                    return;
                }
                if (_knownMsgCounts.TryGetValue(gKey, out int knownG) && knownG >= 0)
                {
                    int diff = fromOthers - knownG;
                    if (diff > 0)
                    {
                        var groups = GroupsList.ItemsSource as List<GroupItem>;
                        var gi = groups?.FirstOrDefault(g => g.Id == groupId);
                        if (gi != null)
                        {
                            gi.UnreadCount = Math.Min(gi.UnreadCount + diff, 99);
                            _ = Dispatcher.InvokeAsync(() => GroupsList.Items.Refresh(), DispatcherPriority.Background);
                        }
                    }
                }
                _knownMsgCounts[gKey] = fromOthers;
            }
        }

        private void ChatHeader_Click(object sender, MouseButtonEventArgs e)
        {
            if (_isCurGroup || string.IsNullOrEmpty(_curHash) || _isChannel) return;
            var ci = (ContactsList.ItemsSource as List<ContactItem>)?.FirstOrDefault(c => c.Hash == _curHash);
            if (ci != null) _ = ShowContactProfile(ci);
        }

        private async Task ShowContactProfile(ContactItem ci)
        {
            ProfileDisplayName.Text = ci.DisplayName;
            ProfileHashDisplay.Text = ci.Hash;
            ProfileStatus.Text = ci.IsOnline ? "Online" : "Offline";
            ProfileAvatarInitials.Text = ci.Initials;
            ProfileAvatarInitials.Visibility = Visibility.Visible;
            ProfileAvatarImg.Visibility = Visibility.Collapsed;
            ProfileMsgCount.Text = "…";

            if (ContactProfileOverlay != null)
            {
                ContactProfileOverlay.Opacity = 0;
                ContactProfileOverlay.Visibility = Visibility.Visible;
            }

            if (ContactProfileOverlay?.Child is Border card)
                PrepareOverlayCard(card, 0.92, 28, 0);

            await Dispatcher.InvokeAsync(() => { }, DispatcherPriority.Render);
            AnimateContactProfileOpen();

            _ = Dispatcher.BeginInvoke(new Action(() =>
            {
                try
                {
                    if (ContactProfileOverlay?.Visibility != Visibility.Visible || ProfileHashDisplay?.Text != ci.Hash)
                        return;

                    if (ci.AvatarSource != null)
                    {
                        ProfileAvatarImg.Source = ci.AvatarSource;
                        ProfileAvatarImg.Clip = new System.Windows.Media.EllipseGeometry(new Point(42, 42), 42, 42);
                        ProfileAvatarImg.Visibility = Visibility.Visible;
                        ProfileAvatarInitials.Visibility = Visibility.Collapsed;
                    }
                    else if (ci.HasAvatar)
                    {
                        _ = LoadProfileAvatarAsync(ci.Hash);
                    }
                }
                catch { }
            }), DispatcherPriority.Background);

            _ = Task.Run(async () =>
            {
                try
                {
                    int msgCount = _lastMessages.Count(m => m.Sender == ci.Hash);
                    var res = await _http.SendAsync(Req(HttpMethod.Get, $"/get_messages/{ci.Hash}"));
                    if (!res.IsSuccessStatusCode) return;
                    int cnt = JsonDocument.Parse(await res.Content.ReadAsStringAsync())
                        .RootElement.GetProperty("messages").EnumerateArray()
                        .Count(m => m.GetProperty("sender").GetString() == ci.Hash);
                    Dispatcher.Invoke(() =>
                    {
                        var overlay = ContactProfileOverlay;
                        var profileHash = ProfileHashDisplay?.Text;
                        if (overlay != null && overlay.Visibility == Visibility.Visible
                            && profileHash == ci.Hash && ProfileMsgCount != null)
                            ProfileMsgCount.Text = cnt > 0 ? cnt.ToString() : msgCount.ToString();
                    });
                }
                catch { }
            });
        }

        private async Task LoadProfileAvatarAsync(string hash)
        {
            try
            {
                var bmp = await GetOrFetchAvatar(hash);
                if (bmp == null) return;
                if (ProfileHashDisplay?.Text != hash || ContactProfileOverlay?.Visibility != Visibility.Visible) return;
                ProfileAvatarImg.Source = bmp;
                ProfileAvatarImg.Clip = new System.Windows.Media.EllipseGeometry(new Point(42, 42), 42, 42);
                ProfileAvatarImg.Visibility = Visibility.Visible;
                ProfileAvatarInitials.Visibility = Visibility.Collapsed;
            }
            catch { }
        }

        private static void PrepareOverlayCard(Border? card, double startScale, double startTranslateY, double startTranslateX = 0)
        {
            if (card == null) return;

            card.Opacity = 0;
            card.RenderTransformOrigin = new Point(0.5, 0.5);

            if (card.RenderTransform is not TransformGroup group ||
                group.Children.Count < 2 ||
                group.Children[0] is not ScaleTransform ||
                group.Children[1] is not TranslateTransform)
            {
                group = new TransformGroup();
                group.Children.Add(new ScaleTransform(startScale, startScale));
                group.Children.Add(new TranslateTransform(startTranslateX, startTranslateY));
                card.RenderTransform = group;
            }
            else
            {
                if (group.Children[0] is ScaleTransform scale)
                {
                    scale.ScaleX = startScale;
                    scale.ScaleY = startScale;
                }
                if (group.Children[1] is TranslateTransform translate)
                {
                    translate.X = startTranslateX;
                    translate.Y = startTranslateY;
                }
            }
        }

        private static void AnimateOverlayOpen(Border overlay, Border? card, double rootMs, double cardMs, double startScale, double startTranslateY, double startTranslateX = 0)
        {
            if (overlay.Visibility != Visibility.Visible)
                return;

            var rootEase = new System.Windows.Media.Animation.SineEase
            { EasingMode = System.Windows.Media.Animation.EasingMode.EaseOut };

            overlay.BeginAnimation(UIElement.OpacityProperty,
                new System.Windows.Media.Animation.DoubleAnimation(0, 1, TimeSpan.FromMilliseconds(rootMs))
                {
                    EasingFunction = rootEase,
                    FillBehavior = System.Windows.Media.Animation.FillBehavior.HoldEnd
                });

            if (card == null)
                return;

            card.Opacity = 0;
            card.RenderTransformOrigin = new Point(0.5, 0.5);

            if (card.RenderTransform is not TransformGroup group ||
                group.Children.Count < 2 ||
                group.Children[0] is not ScaleTransform scale ||
                group.Children[1] is not TranslateTransform tt)
            {
                PrepareOverlayCard(card, startScale, startTranslateY, startTranslateX);
                if (card.RenderTransform is not TransformGroup rebuilt || rebuilt.Children.Count < 2 ||
                    rebuilt.Children[0] is not ScaleTransform rebuiltScale ||
                    rebuilt.Children[1] is not TranslateTransform rebuiltTt)
                    return;
                scale = rebuiltScale;
                tt = rebuiltTt;
            }

            card.BeginAnimation(UIElement.OpacityProperty,
                new System.Windows.Media.Animation.DoubleAnimation(0, 1, TimeSpan.FromMilliseconds(cardMs))
                {
                    EasingFunction = rootEase,
                    FillBehavior = System.Windows.Media.Animation.FillBehavior.HoldEnd
                });

            scale.BeginAnimation(ScaleTransform.ScaleXProperty,
                new System.Windows.Media.Animation.DoubleAnimation(startScale, 1.0, TimeSpan.FromMilliseconds(cardMs))
                {
                    EasingFunction = rootEase,
                    FillBehavior = System.Windows.Media.Animation.FillBehavior.HoldEnd
                });

            scale.BeginAnimation(ScaleTransform.ScaleYProperty,
                new System.Windows.Media.Animation.DoubleAnimation(startScale, 1.0, TimeSpan.FromMilliseconds(cardMs))
                {
                    EasingFunction = rootEase,
                    FillBehavior = System.Windows.Media.Animation.FillBehavior.HoldEnd
                });

            tt.BeginAnimation(TranslateTransform.YProperty,
                new System.Windows.Media.Animation.DoubleAnimation(startTranslateY, 0, TimeSpan.FromMilliseconds(cardMs))
                {
                    EasingFunction = rootEase,
                    FillBehavior = System.Windows.Media.Animation.FillBehavior.HoldEnd
                });

            if (Math.Abs(startTranslateX) > 0.01)
            {
                tt.BeginAnimation(TranslateTransform.XProperty,
                    new System.Windows.Media.Animation.DoubleAnimation(startTranslateX, 0, TimeSpan.FromMilliseconds(cardMs))
                    {
                        EasingFunction = rootEase,
                        FillBehavior = System.Windows.Media.Animation.FillBehavior.HoldEnd
                    });
            }
        }

        private async void AnimateContactProfileOpen()
        {
            var overlay = ContactProfileOverlay;
            var card = overlay?.Child as Border;
            if (overlay == null || overlay.Visibility != Visibility.Visible)
                return;

            await Dispatcher.Yield(DispatcherPriority.Render);
            await Dispatcher.Yield(DispatcherPriority.Background);
            AnimateOverlayOpen(overlay, card, 250, 280, 0.94, 20);
        }
        private void CloseContactProfileOverlay()
        {
            var overlay = ContactProfileOverlay;
            if (overlay == null) return;
            overlay.Visibility = Visibility.Collapsed;
        }

        private void ContactProfileOverlay_BgClick(object sender, MouseButtonEventArgs e)
        { if (e.Source == ContactProfileOverlay) CloseContactProfileOverlay(); }

        private void ContactProfileCard_Click(object sender, MouseButtonEventArgs e) => e.Handled = true;

        private void CloseContactProfile_Click(object sender, MouseButtonEventArgs e)
            => CloseContactProfileOverlay();

        private void ProfileHash_Click(object sender, MouseButtonEventArgs e)
        { var h = ProfileHashDisplay?.Text ?? string.Empty; if (!string.IsNullOrEmpty(h)) { Clipboard.SetText(h); ShowToast("ID copied!"); } }

        private void Logout_Click(object sender, RoutedEventArgs e)
        {
            _pollTimer.Stop(); _statusTimer.Stop(); _heartbeatTimer.Stop();
            _token = null; _myHash = null; _curHash = null; _myDisplayName = null;
            _settings.SavedToken = "";
            _settings.SavedUserHash = "";
            SaveSettingsFile();

            if (MyAvatarImg != null) MyAvatarImg.Source = null;
            _lastMessages.Clear();
            MessagesPanel.Children.Clear();
            _pollRunning = false;
            _knownMsgCounts.Clear();
            _derivedKeyCache.Clear();
            _emojiSplitCache.Clear();
            _translationCache.Clear();
            _translateActive.Clear();
            _lastFullContactsLoad = DateTime.MinValue;
            MainScreen.Visibility = Visibility.Collapsed;
            AuthScreen.Visibility = Visibility.Visible;
        }
    }
}