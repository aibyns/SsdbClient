using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ljlx.Apns
{
    public class ApplePushService
    {
        private static ApplePushService _applePushService;
        private static readonly object SyncObject = new object(); 
        private string HostIp { get; }
        private int Port { get; }
        private readonly Stack<PushMsgModel> _messageList;//消息实体队列 
    
        private const int DeviceTokenBinarySize = 32;
        private readonly X509Certificate _certificate;
        private readonly X509CertificateCollection _x509CertificateCollection;

        /// <summary>
        /// 消息发送失败
        /// </summary> 
        public delegate void ApnsHandler(int state ,string msg);
        /// <summary>
        /// 消息发送失败
        /// </summary>
        public  ApnsHandler ApnsResults { get; set; }
         
        public ApplePushService(string filePath , string pwd , bool dubug = true, int port = 2195)
        { 
            _messageList = new Stack<PushMsgModel>();
 
            // 开发环境还是生成环境
            HostIp = dubug ? "gateway.sandbox.push.apple.com" : "gateway.push.apple.com";
            Port = port;

            var password = pwd;// 苹果证书密码
            var certificatepath = filePath;//苹果证书文件名; 
            var p12Filename = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, certificatepath);
            _certificate = new X509Certificate2(File.ReadAllBytes(p12Filename), password, X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable);

            _x509CertificateCollection = new X509CertificateCollection { _certificate }; 
        }
      
        #region=====公布给外接调用的方法 

        /// <summary>
        /// 获取对象实例
        /// </summary>
        /// <returns></returns>
        public static ApplePushService GetInstance(string filePath, string pwd, bool dubug = true, int port = 2195)
        {
            if (_applePushService != null) return _applePushService;
            lock (SyncObject)
            { 
                _applePushService = new ApplePushService(filePath,pwd,dubug,port);
                _applePushService.TherdStart();
            }
            return _applePushService;
        }

        /// <summary>
        /// 添加需要推送的消息
        /// </summary>
        /// <param name="message">消息体</param>
        /// <param name="expiration"></param>
        public void AddMessage(PushMsgModel message, DateTime expiration)
        {
            _messageList.Push(message);
        }

        /// <summary>
        /// 推送消息
        /// </summary>
        private void SendMessage()
        {
            try
            {
                while (true)
                {
                    if (_messageList.Count > 0)//如果 消息队列中的消息不为零 推送
                    {
                        while (_messageList.Count > 0)
                        {
                            try
                            { 
                                var msg = _messageList.Pop();

                                Task.Factory.StartNew(() =>
                                { 
                                    SendToApns(msg); 
                                }); 
                            }
                            catch (Exception exception)
                            {
                                Console.WriteLine($"【推送消息实体错误】：{exception.Message}");
                            }
                        }

                        Console.WriteLine("执行");
                    }
                    else
                    {
                        Thread.Sleep(5 * 60 * 1000);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"【推送消息线程错误】：{ex.Message}");
            }
        }
        /// <summary>
        /// 启动推送
        /// </summary>
        private void TherdStart()
        {
            var td = new Thread(SendMessage) { IsBackground = true };
            td.Start();
        }
        #endregion

        private void SendToApns(PushMsgModel msg)
        { 
            var tcpClient = new TcpClient();
            tcpClient.Connect(HostIp, Port);
            var sslStream = new SslStream(tcpClient.GetStream(), false, ValidateServerCertificate, SelectLocalCertificate);

            try
            {
                sslStream.AuthenticateAsClient(HostIp, _x509CertificateCollection,
                    System.Security.Authentication.SslProtocols.Tls, false);

                if (!sslStream.IsMutuallyAuthenticated)
                { 
                    ApnsResults?.BeginInvoke(-1, "error:Ssl Stream Failed to Authenticate！", ApnsResultsCallBack, ApnsResults); 
                    return;
                }

                if (!sslStream.CanWrite)
                { 
                    ApnsResults?.BeginInvoke(-2, "error:Ssl Stream is not Writable!", ApnsResultsCallBack, ApnsResults);
                    return;
                }
                
                byte[] bytes;
                var boo = ToBytes(msg, out bytes);
                if (!boo) return;

                var readBuffer = new byte[6];

                var ar = sslStream.BeginRead(readBuffer, 0, 6, asyncResult =>
                {
                    try
                    {
                        sslStream.EndRead(asyncResult);
                        var status = readBuffer[1]; 
                        ApnsResults?.BeginInvoke(status, msg.DeviceToken, ApnsResultsCallBack, ApnsResults);  
                    }
                    catch
                    { 
                        // ignored
                    }
                }, null);

                sslStream.Write(bytes); // 发送   

                if (!ar.IsCompleted)
                {
                    ar.AsyncWaitHandle.WaitOne(500);
                    if (!ar.IsCompleted)
                    {
                        ApnsResults?.BeginInvoke(-5, msg.DeviceToken, ApnsResultsCallBack, ApnsResults);
                      
                        Disconnect(tcpClient, sslStream);
                    }
                }
            }
            catch (Exception ex)
            { 
                ApnsResults?.BeginInvoke(-3, $"error: {ex.Message}", ApnsResultsCallBack, ApnsResults);
            }
        }
         
        void ApnsResultsCallBack(IAsyncResult result)
        {
            ApnsResults.EndInvoke(result);
        }

        /// <summary>
        /// 创建 消息体
        /// </summary>
        /// <param name="msg"></param> 
        /// <param name="sBytes"></param>
        /// <returns></returns>
        private bool ToBytes(PushMsgModel msg, out byte[] sBytes)
        { 
            try
            {  
                var identifier = 0; 
                var identifierBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(identifier));

                var expiryTimeStamp = -1;//过期时间戳
                if (msg.Expiration != DateTime.MinValue)
                {
                    DateTime concreteExpireDateUtc = msg.Expiration.ToUniversalTime();
                    TimeSpan epochTimeSpan = concreteExpireDateUtc - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    expiryTimeStamp = (int)epochTimeSpan.TotalSeconds;
                }
                var expiry = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(expiryTimeStamp));
                var bytes = new byte[32];

                for (var i = 0; i < bytes.Length; i++)
                    bytes[i] = byte.Parse(msg.DeviceToken.Substring(i * 2, 2), System.Globalization.NumberStyles.HexNumber);

                if (bytes.Length != DeviceTokenBinarySize)
                { 
                    ApnsResults?.BeginInvoke(-4, "error: Device token length error！", ApnsResultsCallBack, ApnsResults);

                    sBytes = null;
                    return false;
                }
                var deviceTokenSize = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(Convert.ToInt16(bytes.Length)));
                 
                var str = $"{{\"aps\":{{\"alert\":\"{msg.Alert}\",\"badge\":1,\"sound\":\"default\",\"className\":\"{msg.ClassName}\",\"value\":\"{msg.Values}\",\"storyBoard\":\"{msg.StoryBoard}\",\"version\":\"{msg.Version}\"}}}}";

                var payload = Encoding.UTF8.GetBytes(str);
                var payloadSize = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(Convert.ToInt16(payload.Length)));
                var notificationParts = new List<byte[]>
                {
                    new byte[] {0x01},
                    identifierBytes,
                    expiry,
                    deviceTokenSize,
                    bytes,
                    payloadSize,
                    payload
                };
                sBytes = BuildBufferFrom(notificationParts);
                return true;
            }
            catch (Exception)
            {
                sBytes = null;
                return false;
            }
        } 

        private byte[] BuildBufferFrom(IList<byte[]> bufferParts)
        {
            var bufferSize = bufferParts.Sum(t => t.Length);

            var buffer = new byte[bufferSize];
            var position = 0;
            foreach (var t in bufferParts)
            {
                var part = t;
                Buffer.BlockCopy(t, 0, buffer, position, part.Length);
                position += part.Length;
            }
            return buffer;
        }

        private void  Disconnect(TcpClient tcp, SslStream ssl)
        {
            try
            {
                ssl.Close();
                ssl.Dispose();
            }
            catch
            {
                // ignored
            }
             
            try
            {
                tcp.Client.Shutdown(SocketShutdown.Both);
                tcp.Client.Dispose();
                tcp.Close();
            }
            catch
            {
                // ignored
            }
        }

        private  bool ValidateServerCertificate(object sender, X509Certificate certificate, X509Chain chain, SslPolicyErrors sslPolicyErrors)
        {
            return true;
        }
        private  X509Certificate SelectLocalCertificate(object sender, string targetHost, X509CertificateCollection localCertificates, X509Certificate remoteCertificate, string[] acceptableIssuers)
        {
            return _certificate;
        }
    }

    public static class TcpExtensions
    { 
        public static void SetSocketKeepAliveValues(this TcpClient tcpc, int keepAliveTime, int keepAliveInterval)
        { 
            const uint dummy = 0;  
            var inOptionValues = new byte[System.Runtime.InteropServices.Marshal.SizeOf(dummy) * 3];

            BitConverter.GetBytes((uint)1).CopyTo(inOptionValues, 0);
            BitConverter.GetBytes((uint)keepAliveTime).CopyTo(inOptionValues, System.Runtime.InteropServices.Marshal.SizeOf(dummy));
            BitConverter.GetBytes((uint)keepAliveInterval).CopyTo(inOptionValues, System.Runtime.InteropServices.Marshal.SizeOf(dummy) * 2);
           
            tcpc.Client.IOControl(IOControlCode.KeepAliveValues, inOptionValues, null);
        }
         
        public static bool IsOnline(this TcpClient c)
        {
            return !((c.Client.Poll(1000, SelectMode.SelectRead) && (c.Client.Available == 0)) || !c.Client.Connected);
        }
    }

    public class PushMsgModel
    {
        public string DeviceToken { get; set; }

        public string Alert { get; set; }

        public string ClassName { get; set; }
 
        public string Values { get; set; }

        public string StoryBoard { get; set; }

        public string Version { get; set; }

        public int Badge { get; set; }

        public DateTime Expiration { get; set; } 
    } 
}
