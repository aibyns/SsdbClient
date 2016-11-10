using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ljlx.Apns.Text
{
    class Program
    {
        static void Main(string[] args)
        {
            var uuid = "14a1084bd310151d136fb31a39ee25109d135e98615b8fbf58890f91cad357be";

            // 值 说明 ActivityListVCtrl  跳转到活动列表   ScenicStoryVCtrl 跳转到 景区故事列表   InternetViewCtrl 跳转http 地址  MessageVCtrl  跳转到消息类型   
            // 值 说明 StoryboardHome  跳转到活动列表   ScenicStory 跳转到 景区故事列表   otherMore 跳转http 地址  Message  跳转到消息类型   
            // 消息类型   1消息 2景区故事  3景区活动 
             
            var applePushService = ApplePushService.GetInstance("push.p12", "123456", false);

            applePushService.ApnsResults += ApnsResults; 
            applePushService.AddMessage(new PushMsgModel() { DeviceToken = uuid, Alert = "推送链景头条", Badge = 1, ClassName = "TopStoryViewController", StoryBoard = "", Values = "{\\\"urlString\\\":\\\"\\\",\\\"city\\\":\\\"北京\\\"}", Version = "12", }, DateTime.MinValue);
         
            while (true)
            {
                var chars = Console.ReadLine();
                if (chars == "q") return;
               
            }
        }

        static void ApnsResults(int state, string msg)
        {
            Console.WriteLine($"推送报告： 状态{state}，消息：{msg}");
        } 
    }
}
