package com.zl;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Mr.zhu on 2015/8/25.
 * 对日志数据进行过滤，将合乎规则的记录划分成各个字段并提取出来
 */
public class DataFilter implements Serializable {
    //为指定子系统查找或创建一个logger
    private static final Logger logger = Logger.getLogger("Access");

    private String onlineDetailID; //上网明细ID，eg:2c929293466b97a6014754607e457d68
    private String userID;         //账号，eg:U201215025
    private String userMAC;        //MAC地址，eg:A417314EEA7B
    private String userIPv4;       //用户IPv4地址，eg:10.12.49.26
    private String loginTime;        //上线时间，eg:2014-07-20 22:44:18.540000000
    private String logoutTime;       //下线时间，eg:2014-07-20 23:10:16.540000000
    private long onlineSeconds;  //在线时间，eg:1558
    private int accessType;     //接入类型，eg:15（1有线客户端认证，3有线Web认证，12无线MAC无感认证，13无线AUTO无感认证，15无线Web认证）
    private String userTemplateID; //模版，eg:本科生动态IP模版/研究生动态IP模版
    private String packageName;    //套餐，eg:100元每半年/20元每月/1元每天
    private String serviceSuffix;  //服务，eg:internet

    private DataFilter(String onlineDetailID, String userID, String userMAC, String userIPv4, String loginTime, String logoutTime,
                        String onlineSeconds, String accessType, String userTemplateID, String packageName, String serviceSuffix){
        this.onlineDetailID = onlineDetailID;
        this.userID = userID;
        this.userMAC = userMAC;
        this.userIPv4 = userIPv4;
        this.loginTime = loginTime;
        this.logoutTime = logoutTime;
        this.onlineSeconds = Long.parseLong(onlineSeconds);
        this.accessType = Integer.parseInt(accessType);
        this.userTemplateID = userTemplateID;
        this.packageName = packageName;
        this.serviceSuffix = serviceSuffix;
    }

    public String getOnlineDetailID(){ return onlineDetailID; }

    public String getUserID(){ return userID; }

    public String getUserMAC(){ return userMAC; }

    public String getUserIPv4(){ return userIPv4; }

    public String getLoginTime(){ return loginTime; }

    public String getLogoutTime(){ return logoutTime; }

    public Long getOnlineSeconds(){ return onlineSeconds; }

    public int getAccessType(){ return accessType; }

    public String getUserTemplateID(){ return userTemplateID; }

    public String getPackageName(){ return packageName; }

    public String getServiceSuffix(){ return serviceSuffix; }

    // Example Apache log line:（日志格式）
    // 1、2c929293466b97a6014754607e457d68,上网明细ID
    // 2、U201215025,账号
    // 3、A417314EEA7B,MAC地址
    // 4、10.12.49.26,用户IPv4网络地址
    // 5、2014-07-20
    // 6、22:44:18.540000000,用户上线时间
    // 7、2014-07-20
    // 8、23:10:16.540000000,用户下线时间
    // 9、1558,用户在线时间
    // 10、15,接入类型（1有线客户端认证，3有线Web认证，12无线MAC无感认证，13无线AUTO无感认证，15无线Web认证）
    // 11、本科生动态IP模版,模板
    // 12、100元每半年,套餐
    // 13、internet，服务
    private static final String LOG_ENTRY_PATTERN =
            "^(\\w+),(\\w+),(\\w+),(\\S+),(\\S+) (\\S+),(\\S+) (\\S+),(\\d+),(\\d+),(\\S+),(\\S+),(\\w+)";
    //正则表达式中一个（）就定义了一个组，以便后面Matcher类处理
    //"\s"匹配任何不可见字符，包括空格、制表符、换页符等等。等价于[ \f\n\r\t\v]
    //"\S"匹配任意不是空白符的字符
    //"\w"匹配包括下划线的任何单词字符。类似但不等价于“[A-Za-z0-9_]”，这里的"单词"字符使用Unicode字符集

    //将正则表达式赋予Pttern对象
    private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

    public static DataFilter parseFromLogLine(String dataline) {
        //与日志文件进行匹配
        Matcher m = PATTERN.matcher(dataline);

        if (!m.find()) {//如果有不匹配的则输出错误
            //日志级别ALL表示启用所有消息的日志记录
            logger.log(Level.ALL, "Cannot parse logline" + dataline);
            throw new RuntimeException("Error parsing logline");
        }

        //返回记录的各个字段
        return new DataFilter(m.group(1), m.group(2), m.group(3), m.group(4), m.group(6),
                m.group(8), m.group(9), m.group(10), m.group(11),m.group(12), m.group(13));
    }
}
