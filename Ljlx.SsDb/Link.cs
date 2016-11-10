using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;

namespace Ljlx.SsDb
{
    public class Link
	{
        private TcpClient _sock;
        private MemoryStream _recvBuf = new MemoryStream(8 * 1024);

        public int Id { get; set; } // 标识连接地址

        public bool Status = false;//socket的状态true为空闲，false为忙碌
        public string IpPort { get;set;} // 标识连接地址

        public Link(string host, int port)
        {
            _sock = new TcpClient(host, port) { NoDelay = true };
            _sock.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
        }

        ~Link()
        {
            Close();
        }

        public void Close()
        {
            _sock?.Close();
            _sock = null;
        } 

        public bool WhetherConnection()
        {
            return _sock != null && _sock.Connected;
        }
         
        public List<byte[]> Request(string cmd, params string[] args)
        {
            var req = new List<byte[]>(1 + args.Length) { Encoding.Default.GetBytes(cmd) };
            req.AddRange(args.Select(s => Encoding.Default.GetBytes(s)));
            return Request(req);
        }

        public List<byte[]> Request(string cmd, params byte[][] args)
        {
            var req = new List<byte[]>(1 + args.Length) {Encoding.Default.GetBytes(cmd)};
            req.AddRange(args);
            return Request(req);
        }

        public List<byte[]> Request(List<byte[]> req)
        {
            MemoryStream buf = new MemoryStream();
            foreach (var p in req)
            {
                var len = Encoding.Default.GetBytes(p.Length.ToString());
                buf.Write(len, 0, len.Length);
                buf.WriteByte((byte)'\n');
                buf.Write(p, 0, p.Length);
                buf.WriteByte((byte)'\n');
            }
            buf.WriteByte((byte)'\n');

            var bs = buf.GetBuffer();
            _sock.GetStream().Write(bs, 0, (int)buf.Length);
            return Recv();
        }

        private List<byte[]> Recv()
        {
            while (true)
            {
                var ret = Parse();
                if (ret != null)
                {
                    Status = true;  //socket的状态true为空闲，false为忙碌
                     
                    return ret;
                }
                var bs = new byte[8192];
                var len = _sock.GetStream().Read(bs, 0, bs.Length);
                _recvBuf.Write(bs, 0, len);
            }
        }

        private static int Memchr(byte[] bs, byte b, int offset)
        {
            for (int i = offset; i < bs.Length; i++)
            {
                if (bs[i] == b)
                {
                    return i;
                }
            }
            return -1;
        }

        private List<byte[]> Parse()
        {
            var list = new List<byte[]>();
            var buf = _recvBuf.GetBuffer();

            var idx = 0;
            while (true)
            {
                var pos = Memchr(buf, (byte)'\n', idx);
                if (pos == -1)
                {
                    break;
                }
                if (pos == idx || (pos == idx + 1 && buf[idx] == '\r'))
                {
                    idx += 1;
                    if (list.Count == 0)
                    {
                        continue;
                    }
                    var left = (int)_recvBuf.Length - idx;
                    _recvBuf = new MemoryStream(8192);
                    if (left > 0)
                    {
                        _recvBuf.Write(buf, idx, left);
                    }
                    return list;
                }
                var lens = new byte[pos - idx];
                Array.Copy(buf, idx, lens, 0, lens.Length);
                var len = int.Parse(Encoding.Default.GetString(lens));

                idx = pos + 1;
                if (idx + len >= _recvBuf.Length)
                {
                    break;
                }
                var data = new byte[len];
                Array.Copy(buf, idx, data, 0, data.Length);
                idx += len + 1;
                list.Add(data);
            }
            return null;
        }
    }
}
