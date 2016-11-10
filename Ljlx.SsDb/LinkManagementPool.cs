using System;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;

namespace Ljlx.SsDb
{
    public class LinkManagementPool
    {

        private static LinkManagementPool _linkManagementPool;
        private static readonly object SyncObject = new object();
        private static string [] _writeIps;
        private static string [] _readIps;

        public  int OneConnectionPoolSize { get; } = 3;
         
        private static  Dictionary<int, Link> _wSocketPool; // 连接池 
        protected static int WritePoolIndex;

        private static  Dictionary<int, Link> _rSocketPool; // 连接池
        protected static int ReadPoolIndex;

        private static readonly Mutex MMutex = new Mutex();

        private static int MTimeout = 10;

        public static Timer MTimer { get; set; }

        public static List<string> FaultWriteIps { get; private set; }
        public static List<string> FaultReadIps { get; private set; }
         
        /// <summary>
        /// 获取对象实例
        /// </summary>
        /// <returns></returns>
        public static LinkManagementPool GetInstance(string[] writeIps, string[] readIps)
        {
            if (_linkManagementPool != null) return _linkManagementPool;

            lock (SyncObject)
            { 
                _linkManagementPool = new LinkManagementPool(writeIps, readIps);
                _linkManagementPool.Init();
            }

            return _linkManagementPool;
        }

        public LinkManagementPool(string[] writeIps, string[] readIps)
        {
            _writeIps = writeIps;
            _readIps = readIps;

            FaultWriteIps = new List<string>();
            FaultReadIps = new List<string>();

            WritePoolIndex = 0;
            ReadPoolIndex = 0; 
        }

        public void Init()
        {
            #region 处理读 连接 
            _rSocketPool = new Dictionary<int, Link>();

            foreach (var readIp in _readIps)
            {
                var ipInfo = readIp.Split(':');
                if (ipInfo.Length < 2) continue;

                var ip = ipInfo[0];
                var port = ipInfo[1];

                for (var i = 0; i < OneConnectionPoolSize; i++)
                {
                    var link = new Link(ip, int.Parse(port)) { Status = true, IpPort = readIp };
                    _rSocketPool.Add(i, link);
                }
            }
            #endregion 

            #region 处理写 连接 
            _wSocketPool = new Dictionary<int, Link>();

            foreach (var writeIp in _writeIps)
            {
                var ipInfo = writeIp.Split(':');
                if (ipInfo.Length<2) continue;

                var ip = ipInfo[0];
                var port = ipInfo[1];

                for (var i = 0; i < OneConnectionPoolSize; i++)
                {
                    var link = new Link(ip, int.Parse(port)) {Status = true, IpPort= writeIp };
                    _wSocketPool.Add(i,link);
                } 
            }
            #endregion

            MTimer = new Timer(CheckService, null, MTimeout * 1000, MTimeout * 1000); // 超时时间以毫秒为单位  
        }

        /// <summary>
        /// 获取 写的连接
        /// </summary>
        /// <returns></returns>
        public  Link GetWriteLink()
        {
            MMutex.WaitOne(); //先阻塞
            if (WritePoolIndex >= _wSocketPool.Count) WritePoolIndex = 0;

            for (var i = WritePoolIndex; i < _wSocketPool.Count; i++)
            {
                if (!_wSocketPool[i].Status) continue;
                _wSocketPool[i].Status = false;
                MMutex.ReleaseMutex();//释放资源
                WritePoolIndex ++;
                return _wSocketPool[i];
            }
            //如果没有空闲的链接，要么等待，要么程序再动态创建一个链接。 
            MMutex.ReleaseMutex();//释放资源 

            try
            {
                if (WritePoolIndex < _wSocketPool.Count)
                {
                    var wSockett = _wSocketPool[ReadPoolIndex];
                    var ipInfo = wSockett.IpPort.Split(':');
                    if (ipInfo.Length > 1)
                    {
                        var ip = ipInfo[0];
                        var port = ipInfo[1];
                        var link = new Link(ip, int.Parse(port)) { Status = true, IpPort = wSockett.IpPort };

                        _wSocketPool.Add(_wSocketPool.Count, link);

                        return link;
                    }
                }
            }
            catch (Exception)
            {
                return null;
            }
            return null;
        }

        /// <summary>
        /// 获取读的连接
        /// </summary>
        /// <returns></returns>
        public  Link GetReadLink()
        {
            MMutex.WaitOne(); //先阻塞  
            if (ReadPoolIndex >= _rSocketPool.Count) ReadPoolIndex = 0;

            for (var i = 0; i < _rSocketPool.Count; i++)
            {
                if (!_rSocketPool[i].Status) continue;
                _rSocketPool[i].Status = false;
                MMutex.ReleaseMutex();//释放资源 
                ReadPoolIndex ++;
                return _rSocketPool[i];
            }
            //如果没有空闲的链接，要么等待，要么程序再动态创建一个链接。 
            MMutex.ReleaseMutex();//释放资源 

            try
            {
                if (ReadPoolIndex < _rSocketPool.Count)
                {
                    var rSockett = _rSocketPool[ReadPoolIndex];
                    var ipInfo = rSockett.IpPort.Split(':');
                    if (ipInfo.Length > 1)
                    {
                        var ip = ipInfo[0];
                        var port = ipInfo[1]; 
                        var link = new Link(ip, int.Parse(port)) { Status = true, IpPort = rSockett.IpPort };

                        _rSocketPool.Add(_rSocketPool.Count,link);

                        return link;
                    } 
                } 
            }
            catch (Exception)
            {
                return null;
            } 
            return null;
        }

        /// <summary>
        /// 释放一个写连接
        /// </summary>
        /// <param name="client"></param>
        public void DisposeWriteLink(Link client)
        {
            try
            {
                for (var i = _wSocketPool.Count - 1; i >= 0; i--)
                {
                    if (client.Id != _wSocketPool[i].Id) continue;

                    _wSocketPool[i].Close();
                    _wSocketPool[i] = null;
                    _wSocketPool.Remove(i);
                }

                var ipInfo = client.IpPort.Split(':');
                if (ipInfo.Length < 2) return;

                var ip = ipInfo[0];
                var port = ipInfo[1];

                var link = new Link(ip, int.Parse(port)) { Status = true, IpPort = client.IpPort };
                _wSocketPool.Add(_wSocketPool.Count, link);
            }
            catch (Exception)
            {
                
                throw;
            }
           
        }

        /// <summary>
        /// 释放一个读连接
        /// </summary>
        /// <param name="client"></param>
        public void DisposeReadLink(Link client)
        {
            try
            {
                for (var i = _rSocketPool.Count - 1; i >= 0; i--)
                {
                    if (client.Id != _rSocketPool[i].Id) continue;

                    _rSocketPool[i].Close();
                    _rSocketPool[i] = null;
                    _rSocketPool.Remove(i);
                }

                var ipInfo = client.IpPort.Split(':');
                if (ipInfo.Length < 2) return;

                var ip = ipInfo[0];
                var port = ipInfo[1];
                 
                var link = new Link(ip, int.Parse(port)) { Status = true, IpPort = client.IpPort };
                _rSocketPool.Add(_rSocketPool.Count, link);
               
            }
            catch (Exception)
            { 
                throw;
            } 
        }

        /// <summary>
        /// 去除故障服务器
        /// </summary>
        /// <param name="ipPort"></param>
        public void DelFaultService(string ipPort)
        {
            lock (_rSocketPool)
            {
                for (var i = _rSocketPool.Count -1 ; i >= 0; i--)
                { 
                    if (ipPort != _rSocketPool[i].IpPort) continue;

                    _rSocketPool[i].Close();
                    _rSocketPool[i] = null;
                    _rSocketPool.Remove(i);
                }
            }

            lock (_wSocketPool)
            {
                for (var i = _wSocketPool.Count -1 ; i >=0 ; i--)
                { 
                    if (ipPort != _wSocketPool[i].IpPort) continue;

                    _wSocketPool[i].Close();
                    _wSocketPool[i] = null;
                    _wSocketPool.Remove(i);
                }
            } 
        }
            
        /// <summary>
        /// 检查服务器状态
        /// </summary> 
        public void CheckService(object obj)
        { 
            var pingSender = new Ping();

            var data = "sendData:yjly";
            var buffer = Encoding.UTF8.GetBytes(data);
            var timeout = 120;

            foreach (var ipPort in _readIps)  // 检查 写服务器是否 正常
            {
                var ipInfo = ipPort.Split(':');
                if (ipInfo.Length < 2) continue;

                var ip = ipInfo[0];
                var port = ipInfo[1];

                var reply = pingSender.Send(ip, timeout, buffer);

                if (reply != null && reply.Status == IPStatus.Success)
                {
                    if (FaultReadIps.Contains(ipPort))  // 如果存在故障组 则表明一恢复正常
                    { 
                        for (var i = _rSocketPool.Count; i < OneConnectionPoolSize; i++)
                        {
                            var link = new Link(ip, int.Parse(port)) { Status = true, IpPort = ipPort };
                            _rSocketPool.Add(i, link);
                        }

                        FaultReadIps.Remove(ipPort);
                    }
                  
                    continue; // 能ping通  
                }

                FaultReadIps.Add(ipPort);  // 添加到故障组 
                DelFaultService(ipPort);   // 断开故障连接
            }

            foreach (var ipPort in _writeIps)  // 检查 写服务器是否 正常
            {
                var ipInfo = ipPort.Split(':');
                if (ipInfo.Length < 2) continue;

                var ip = ipInfo[0];
                var port = ipInfo[1];

                var reply = pingSender.Send(ip, timeout, buffer);
                if (reply != null && reply.Status == IPStatus.Success)
                {
                    if (FaultWriteIps.Contains(ipPort)) // 如果存在故障组 则表明一恢复正常
                    {
                        for (var i = _wSocketPool.Count; i < OneConnectionPoolSize; i++)
                        {
                            var link = new Link(ip, int.Parse(port)) { Status = true, IpPort = ipPort };
                            _wSocketPool.Add(i, link);
                        }

                        FaultWriteIps.Remove(ipPort);
                    }
                    continue; // 能ping通  
                }

                FaultWriteIps.Add(ipPort);  // 添加到故障组 
                DelFaultService(ipPort);
            } 
        }

        /// <summary>
        /// 检查连接池 信息
        /// </summary>
        public void CheckLinkManagementPoolInfo()
        {
            Console.WriteLine($"读连接池数量：{_rSocketPool.Count}，当前位置{ReadPoolIndex}");
           // Console.WriteLine($"写连接池数量：{_wSocketPool.Count}，当前位置{WritePoolIndex}");
        }

    }
}
