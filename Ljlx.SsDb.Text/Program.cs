using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Ljlx.SsDb.Text
{
    class Program
    {
        static void Main(string[] args)
        {
            //var list = new List<KeyValuePair<string, long>>();
            //for (int i = 0; i < 1000000; i++)
            //{
            //    list.Add(new KeyValuePair<string, long>(i.ToString(), 1));
            //}
            Stopwatch stopwatch = new Stopwatch();

            for (int i = 0; i < 4; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    while (true)
                    { 
                        WorR();  
                        Thread.Sleep(1000);
                    } 
                }); 
            } 
            
            Console.ReadLine(); 
        }


        public static void WorR()
        {
            //  处理时间 统计
            Stopwatch stopwatch = new Stopwatch();

            stopwatch.Start(); //  开始监视代码运行时间   

            SsDbStorage client = new SsDbStorage();

            string val;
            //client.KvSet("a", "23");

            client.KvGet("a", out val);
            //Console.WriteLine(val);

            //Console.WriteLine("-----------------");
            //{
            //    var kvs = client.KvScan("", "", 2);
            //    Console.WriteLine(kvs.Length + " kvs");
            //    foreach (var kv in kvs)
            //    {
            //        Console.WriteLine("    " + kv.Key + ": " + Encoding.Default.GetString(kv.Value));
            //    }
            //}
            //Console.WriteLine("-----------------");
            //{
            //    var kvs = client.Zscan("SCount", "", long.MinValue, long.MaxValue, 3000);
            //    Console.WriteLine(kvs.Length + " kvs");
            //    foreach (var kv in kvs)
            //    {
            //        Console.WriteLine("    " + kv.Key + ": " + kv.Value);
            //    }
            //}
            //Console.WriteLine("-----------------");
            //{
            //    var kvs = client.Zrscan("SCount", "", long.MaxValue, long.MinValue, 3000);
            //    Console.WriteLine(kvs.Length + " kvs");
            //    foreach (var kv in kvs)
            //    {
            //        Console.WriteLine("    " + kv.Key + ": " + kv.Value);
            //    }
            //}

            //Console.WriteLine(client.Zsize("SCount").ToString());
          
            //Console.WriteLine("======================================================= ");

            stopwatch.Stop(); //  停止监视      

            var timespan = stopwatch.Elapsed;
            double hours = timespan.Hours; // 总小时
            double minutes = timespan.Minutes;  // 总分钟
            double seconds = timespan.Seconds;  //  总秒数
            double milliseconds = timespan.Milliseconds;  //  总毫秒数 


            //Console.WriteLine($"【处理时间】：{hours}时{minutes}分{seconds}秒{milliseconds}毫秒- {DateTime.Now.ToString("u")}");

            client.CheckLinkManagementPoolInfo();
        }


        public static void WorR1()
        {
            for (var i = 0; i < 1000; i++)
            {
                //Task.Factory.StartNew(() =>
                //{   //  处理时间 统计
                    Stopwatch stopwatch = new Stopwatch();

                    stopwatch.Start(); //  开始监视代码运行时间   

                    SsDbStorage client = new SsDbStorage();

                    string val;
                    client.KvSet("a", "23");

                    client.KvGet("a", out val);
                    Console.WriteLine(val);

                    Console.WriteLine("-----------------");
                    {
                        var kvs = client.KvScan("", "", 2);
                        Console.WriteLine(kvs.Length + " kvs");
                        foreach (var kv in kvs)
                        {
                            Console.WriteLine("    " + kv.Key + ": " + Encoding.Default.GetString(kv.Value));
                        }
                    }
                    Console.WriteLine("-----------------");
                    {
                        var kvs = client.Zscan("SCount", "", long.MinValue, long.MaxValue, 3000);
                        Console.WriteLine(kvs.Length + " kvs");
                        foreach (var kv in kvs)
                        {
                            Console.WriteLine("    " + kv.Key + ": " + kv.Value);
                        }
                    }
                    Console.WriteLine("-----------------");
                    {
                        var kvs = client.Zrscan("SCount", "", long.MaxValue, long.MinValue, 3000);
                        Console.WriteLine(kvs.Length + " kvs");
                        foreach (var kv in kvs)
                        {
                            Console.WriteLine("    " + kv.Key + ": " + kv.Value);
                        }
                    }

                    Console.WriteLine(client.Zsize("SCount").ToString());
                   
                    Console.WriteLine("======================================================= ");

                    stopwatch.Stop(); //  停止监视      

                    var timespan = stopwatch.Elapsed;
                    double hours = timespan.Hours; // 总小时
                    double minutes = timespan.Minutes;  // 总分钟
                    double seconds = timespan.Seconds;  //  总秒数
                    double milliseconds = timespan.Milliseconds;  //  总毫秒数 
                 
                    Console.WriteLine($"【处理时间】：{hours}时{minutes}分{seconds}秒{milliseconds}毫秒- {DateTime.Now.ToString("u")}");
                    client.CheckLinkManagementPoolInfo();
                //});
            } 
        }
    }
}
