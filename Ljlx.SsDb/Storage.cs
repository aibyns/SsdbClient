using System;
using System.Collections.Generic;
using System.Text;

namespace Ljlx.SsDb
{
	public class SsDbStorage { 
		private string _respCode;
         
        private static readonly string[] Write = "127.0.0.1:8888".Split(',');
        private static readonly string[] Read = "127.0.0.1:8888".Split(',');
 
        public static LinkManagementPool Prcm = LinkManagementPool.GetInstance(Write, Read);
       
        #region 内部帮助 
        private void assert_ok()
        {
            if (_respCode != "ok")
            {
                throw new Exception(_respCode);
            }
        }

        private byte[] _bytes(string s)
        {
            return Encoding.Default.GetBytes(s);
        }

        private string _string(byte[] bs)
        {
            return Encoding.Default.GetString(bs);
        }

        public KeyValuePair<string, byte[]>[] parse_scan_resp(List<byte[]> resp)
        {
            _respCode = _string(resp[0]);
            assert_ok();

            var size = (resp.Count - 1) / 2;
            var kvs = new KeyValuePair<string, byte[]>[size];
            for (var i = 0; i < size; i += 1)
            {
                var key = _string(resp[i * 2 + 1]);
                var val = resp[i * 2 + 2];
                kvs[i] = new KeyValuePair<string, byte[]>(key, val);
            }
            return kvs;
        }

        private List<byte[]> WriteRequest(string cmd, params string[] args)
        {
            Link link = null; 
            while (link == null || !link.WhetherConnection())
            {
                if (link != null)
                {
                    Prcm.DisposeWriteLink(link);
                }
                link = Prcm.GetWriteLink();
            }
            return link.Request(cmd, args);
        }

        private List<byte[]> WriteRequest(string cmd, params byte[][] args)
        {
            Link link = null; 
            while (link == null || !link.WhetherConnection())
            {
                if (link != null)
                {
                    Prcm.DisposeWriteLink(link);
                }
                link = Prcm.GetWriteLink();
            }
            return link.Request(cmd, args);
        }
         
        private List<byte[]> ReadRequest(string cmd, params string[] args)
        {
            Link link =null; 
            while (link==null || !link.WhetherConnection())
            {
                if (link != null)
                {
                    Prcm.DisposeReadLink(link);
                }
                link = Prcm.GetReadLink();
            } 
            return link.Request(cmd, args);
        }

        private List<byte[]> ReadRequest(string cmd, params byte[][] args)
        {
            Link link = null; 
            while (link == null || !link.WhetherConnection())
            {
                if (link != null)
                {
                    Prcm.DisposeReadLink(link);
                }
                link = Prcm.GetReadLink();
            }
            return link.Request(cmd, args);
        }

        #endregion

        #region kv 存储  
        private bool KvExists(byte[] key) {
			var resp = ReadRequest("exists", key);
			_respCode = _string(resp[0]);
			if (_respCode == "not_found")
			{
				return false;
			}
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return _string(resp[1]) == "1";
		}

		public bool KvExists(string key) {
			return KvExists(_bytes(key));
		}

        private void KvSet(byte[] key, byte[] val) {
			var resp = WriteRequest("set", key, val );
			_respCode = _string(resp[0]);
			assert_ok();
		} 
    
        public void KvSet(string key, string val) { 
            KvSet(_bytes(key), _bytes(val)); 
        }

        private bool KvGet(byte[] key, out byte[] val) {
			val = null;
			var resp = ReadRequest("get", key);
			_respCode = _string(resp[0]);
			if (_respCode == "not_found")
			{
				return false;
			}
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			val = resp[1];
			return true;
		}

		public bool KvGet(string key, out byte[] val) {
			return KvGet(_bytes(key), out val);
		}

		public bool KvGet(string key, out string val) {
			val = null;
			byte[] bs;
			if (!KvGet(key, out bs))
			{
				return false;
			}
			val = _string(bs);
			return true;
		}

        private void KvDel(byte[] key) {
			var resp = WriteRequest("del", key);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void KvDel(string key) {
            KvDel(_bytes(key));
		}

		public KeyValuePair<string, byte[]>[] KvScan(string keyStart, string keyEnd, long limit) {
			var resp = ReadRequest("scan", keyStart, keyEnd, limit.ToString());
			return parse_scan_resp(resp);
		}

		public KeyValuePair<string, byte[]>[] KvRscan(string keyStart, string keyEnd, long limit) {
			var resp = ReadRequest("rscan", keyStart, keyEnd, limit.ToString());
			return parse_scan_resp(resp);
		}

        #endregion

        #region hash 存储 
        private void Hset(byte[] name, byte[] key, byte[] val) {
			var resp = WriteRequest("hset", name, key, val);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void Hset(string name, string key, byte[] val) {
			Hset(_bytes(name), _bytes(key), val);
		}

		public void Hset(string name, string key, string val) {
			Hset(_bytes(name), _bytes(key), _bytes(val));
		}
         
        private bool Hget(byte[] name, byte[] key, out byte[] val) {
			val = null;
			var resp = ReadRequest("hget", name, key);
			_respCode = _string(resp[0]);
			if (_respCode == "not_found")
			{
				return false;
			}
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			val = resp[1];
			return true;
		}

		public bool Hget(string name, string key, out byte[] val) {
			return Hget(_bytes(name), _bytes(key), out val);
		}

		public bool Hget(string name, string key, out string val) {
			val = null;
			byte[] bs;
			if (!Hget(name, key, out bs))
			{
				return false;
			}
			val = _string(bs);
			return true;
		}

        private void Hdel(byte[] name, byte[] key) {
			var resp = WriteRequest("hdel", name, key);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void Hdel(string name, string key) {
            Hdel(_bytes(name), _bytes(key));
		}

        private bool Hexists(byte[] name, byte[] key) {
			var resp = ReadRequest("hexists", name, key);
			_respCode = _string(resp[0]);
			if (_respCode == "not_found")
			{
				return false;
			}
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return _string(resp[1]) == "1";
		}
		
		public bool Hexists(string name, string key) {
			return Hexists(_bytes(name), _bytes(key));
		}

        private long Hsize(byte[] name) {
			var resp = ReadRequest("hsize", name);
			_respCode = _string(resp[0]);
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return long.Parse(_string(resp[1]));
		}

		public long Hsize(string name) {
			return Hsize(_bytes(name));
		}

		public KeyValuePair<string, byte[]>[] Hscan(string name, string keyStart, string keyEnd, long limit) {
			var resp = ReadRequest("hscan", name, keyStart, keyEnd, limit.ToString());
			return parse_scan_resp(resp);
		}

		public KeyValuePair<string, byte[]>[] Hrscan(string name, string keyStart, string keyEnd, long limit) {
			var resp = ReadRequest("hrscan", name, keyStart, keyEnd, limit.ToString());
			return parse_scan_resp(resp);
		}

        private void MultiHset(byte[] name, KeyValuePair<byte[], byte[]>[] kvs)
		{
			var req = new byte[(kvs.Length * 2) + 1][];
			req[0] = name;
			for (var i = 0; i < kvs.Length; i++)
			{
				req[(2 * i) + 1] = kvs[i].Key;
				req[(2 * i) + 2] = kvs[i].Value;

			}
			var resp = WriteRequest("multi_hset", req);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void MultiHset(string name, KeyValuePair<string, string>[] kvs)
		{
			var req = new KeyValuePair<byte[], byte[]>[kvs.Length];
			for (var i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], byte[]>(_bytes(kvs[i].Key), _bytes(kvs[i].Value));
			}
            MultiHset(_bytes(name), req);
		}

        private void MultiHdel(byte[] name, byte[][] keys)
		{
			var req = new byte[keys.Length + 1][];
			req[0] = name;
			for (var i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			var resp = WriteRequest("multi_hdel", req);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void MultiHdel(string name, string[] keys)
		{
			var req = new byte[keys.Length][];
			for (var i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
            MultiHdel(_bytes(name), req);
		}

        private KeyValuePair<string, byte[]>[] MultiHget(byte[] name, byte[][] keys)
		{
			var req = new byte[keys.Length + 1][];
			req[0] = name;
			for (var i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			var resp = ReadRequest("multi_hget", req);
			var ret = parse_scan_resp(resp); 
			return ret;
		}

		public KeyValuePair<string, byte[]>[] MultiHget(string name, string[] keys)
		{
			var req = new byte[keys.Length][];
			for (var i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return MultiHget(_bytes(name), req);
		}

        #endregion

        #region zset 有序集合  
        private void Zset(byte[] name, byte[] key, long score) {
			var resp = WriteRequest("zset", name, key, _bytes(score.ToString()));
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void Zset(string name, string key, long score) {
            Zset(_bytes(name), _bytes(key), score);
		}

        private long Zincr(byte[] name, byte[] key, long increment) {
			var resp = WriteRequest("zincr", name, key, _bytes(increment.ToString()));
			_respCode = _string(resp[0]);
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return long.Parse(_string(resp[1]));
		}
		
		public long Zincr(string name, string key, long increment) {
			return Zincr(_bytes(name), _bytes(key), increment);
		}
         
        private bool Zget(byte[] name, byte[] key, out long score) {
			score = -1;
			var resp = ReadRequest("zget", name, key);
			_respCode = _string(resp[0]);
			if (_respCode == "not_found")
			{
				return false;
			}
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			score = long.Parse(_string(resp[1]));
			return true;
		}

		public bool Zget(string name, string key, out long score) {
			return Zget(_bytes(name), _bytes(key), out score);
		}

        private void Zdel(byte[] name, byte[] key) {
			var resp = WriteRequest("zdel", name, key);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void Zdel(string name, string key) {
            Zdel(_bytes(name), _bytes(key));
		}

        private long Zsize(byte[] name) {
			var resp = WriteRequest("zsize", name);
			_respCode = _string(resp[0]);
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return long.Parse(_string(resp[1]));
		}

		public long Zsize(string name) {
			return Zsize(_bytes(name));
		}

        private bool Zexists(byte[] name, byte[] key) {
			var resp = ReadRequest("zexists", name, key);
			_respCode = _string(resp[0]);
			if (_respCode == "not_found")
			{
				return false;
			}
			assert_ok();
			if (resp.Count != 2)
			{
				throw new Exception("Bad response!");
			}
			return _string(resp[1]) == "1";
		}
		
		public bool Zexists(string name, string key) {
			return Zexists(_bytes(name), _bytes(key));
		}

		public KeyValuePair<string, long>[] Zrange(string name, int offset, int limit) {
			var resp = ReadRequest("zrange", name, offset.ToString(), limit.ToString());
			var kvs = parse_scan_resp(resp);
			var ret = new KeyValuePair<string, long>[kvs.Length];
			for (var i = 0; i < kvs.Length; i++)
			{
				var key = kvs[i].Key;
				var score = long.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, long>(key, score);
			}
			return ret;
		}
		
		public KeyValuePair<string, long>[] Zrrange(string name, int offset, int limit) {
			var resp = ReadRequest("zrrange", name, offset.ToString(), limit.ToString());
			var kvs = parse_scan_resp(resp);
			var ret = new KeyValuePair<string, long>[kvs.Length];
			for (var i = 0; i < kvs.Length; i++)
			{
				var key = kvs[i].Key;
				var score = long.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, long>(key, score);
			}
			return ret;
		}

		public KeyValuePair<string, long>[] Zscan(string name, string keyStart, long scoreStart, long scoreEnd, long limit) {
			var scoreS = "";
			var scoreE = "";
			if (scoreStart != long.MinValue)
			{
				scoreS = scoreStart.ToString();
			}
			if (scoreEnd != long.MaxValue)
			{
				scoreE = scoreEnd.ToString();
			}
			var resp = ReadRequest("zscan", name, keyStart, scoreS, scoreE, limit.ToString());
			var kvs = parse_scan_resp(resp);
			var ret = new KeyValuePair<string, long>[kvs.Length];
			for (var i = 0; i < kvs.Length; i++)
			{
				var key = kvs[i].Key;
				var score = long.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, long>(key, score);
			}
			return ret;
		}

		public KeyValuePair<string, long>[] Zrscan(string name, string keyStart, long scoreStart, long scoreEnd, long limit) {
			var scoreS = "";
			var scoreE = "";
			if (scoreStart != long.MaxValue)
			{
				scoreS = scoreStart.ToString();
			}
			if (scoreEnd != long.MinValue)
			{
				scoreE = scoreEnd.ToString();
			}
			var resp = ReadRequest("zrscan", name, keyStart, scoreS, scoreE, limit.ToString());
			var kvs = parse_scan_resp(resp);
			var ret = new KeyValuePair<string, long>[kvs.Length];
			for (var i = 0; i < kvs.Length; i++)
			{
				var key = kvs[i].Key;
				var score = long.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, long>(key, score);
			}
			return ret;
		}

        private void MultiZset(byte[] name, KeyValuePair<byte[], long>[] kvs)
		{
			var req = new byte[(kvs.Length * 2) + 1][];
			req[0] = name;
			for (var i = 0; i < kvs.Length; i++)
			{
				req[(2 * i) + 1] = kvs[i].Key;
				req[(2 * i) + 2] = _bytes(kvs[i].Value.ToString());

			}
			var resp = WriteRequest("multi_zset", req);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void MultiZset(string name, KeyValuePair<string, long>[] kvs)
		{
			var req = new KeyValuePair<byte[], long>[kvs.Length];
			for (var i = 0; i < kvs.Length; i++)
			{
				req[i] = new KeyValuePair<byte[], long>(_bytes(kvs[i].Key), kvs[i].Value);
			}
            MultiZset(_bytes(name), req);
		}

        private void MultiZdel(byte[] name, byte[][] keys)
		{
			var req = new byte[keys.Length + 1][];
			req[0] = name;
			for (var i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			var resp = WriteRequest("multi_zdel", req);
			_respCode = _string(resp[0]);
			assert_ok();
		}

		public void MultiZdel(string name, string[] keys)
		{
			var req = new byte[keys.Length][];
			for (var i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
            MultiZdel(_bytes(name), req);
		}

        private KeyValuePair<string, long>[] MultiZget(byte[] name, byte[][] keys)
		{
			var req = new byte[keys.Length + 1][];
			req[0] = name;
			for (int i = 0; i < keys.Length; i++)
			{
				req[i + 1] = keys[i];
			}
			var resp = ReadRequest("multi_zget", req);
			var kvs = parse_scan_resp(resp);
			var ret = new KeyValuePair<string, long>[kvs.Length];
			for (var i = 0; i < kvs.Length; i++)
			{
				var key = kvs[i].Key;
				var score = long.Parse(_string(kvs[i].Value));
				ret[i] = new KeyValuePair<string, long>(key, score);
			}
			return ret;
		}

		public KeyValuePair<string, long>[] MultiZget(string name, string[] keys)
		{
			var req = new byte[keys.Length][];
			for (var i = 0; i < keys.Length; i++)
			{
				req[i] = _bytes(keys[i]);
			}
			return MultiZget(_bytes(name), req);
        }

        #endregion

        #region 检查连接池 信息

	    public void CheckLinkManagementPoolInfo()
	    {
	        Prcm.CheckLinkManagementPoolInfo();
	    }

	    #endregion
    }
}



