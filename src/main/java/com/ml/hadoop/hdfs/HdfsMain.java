package com.ml.hadoop.hdfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static com.ml.hadoop.hdfs.CommonConfiguration.*;

public class HdfsMain {
    //初始化配置对象，并加载配置文件。
    Configuration conf;
    public void confLoad(){
        conf = new Configuration();
        //conf file
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_HDFS_CORE_XML));
    }
    //配置访问HDFS安全认证
    public void authentication() throws IOException{
        //security mode
        if("kerberos".equalsIgnoreCase(conf.get(""))){
            String PATH_TO_KEYTAB =  HdfsMain.class.getClassLoader().getResource("user.keytab").getPath();
            String PATH_TO_KBR5_CONF =  HdfsMain.class.getClassLoader().getResource("kbr5.conf").getPath();
            System.setProperty("java.security.krb5.conf",PATH_TO_KBR5_CONF);

        }
    }
}
