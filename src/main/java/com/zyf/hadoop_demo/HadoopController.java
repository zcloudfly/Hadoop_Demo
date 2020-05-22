package com.zyf.hadoop_demo;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.web.bind.annotation.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName HadoopController
 * @Description Hadoop的hdfs的一些简单操作
 **/
@RestController
@RequestMapping("/HadoopController")
public class HadoopController {
    private Configuration conf;
    private FileSystem fs;
    public void connection() {
        //设置远程登录的用户（即服务器上操作hadoop的用户），默认是当前机器的用户，如果不设置会报拒绝登录的错误
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        try {
            ugi.doAs((PrivilegedExceptionAction) () -> {
                try {
                    //将hdfs的两个配置文件(和linux下的一样)放到resource目录下，new Configuration()的时候会自动读取，这种方法最简单
                    conf = new Configuration();
                    //设置集群别名，而不是具体的地址，避免硬编码，它会自动的选择active节点进行操作
                    conf.set("fs.defaultFS", "hdfs://192.168.91.134:9000");
                    conf.set("hadoop.job.ugi", "root");
                     fs = FileSystem.get(conf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void close() throws IOException {
        fs.close();
    }
    //查看文件夹下的文件
    @GetMapping("/ls")
    public String homeRoot(String path) throws Exception {
        connection();
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(path), true);
        List list=new ArrayList<String>();
        while(iterator.hasNext()){
            LocatedFileStatus next = iterator.next();
            list.add(next.getPath().getName());
        }
        close();
        return JSON.toJSON(list).toString();
    }
    //创建文件夹
    @PostMapping("/mkdir")
    public String mkdir(String mkdirPath) throws Exception {
        connection();
        boolean b = fs.mkdirs(new Path(mkdirPath));
        close();
        return "创建:" + b;
    }

    //下载文件
    @PostMapping("/downloadFile")
    public String downloadFile(String inPath, String outPath) throws Exception {
        connection();
        FSDataInputStream in = fs.open(new Path(inPath));
        //按行读取打印
        int i=0;
        while(true){
            String line=in.readLine();
            if(StringUtils.isBlank(line)){
                break;
            }
            System.out.println(String.format("第%d行:",++i)+line);
        }
        //如果输出文件路径不为空，文件输出到终端指定文件
        if(StringUtils.isNotEmpty(outPath)) {
            in = fs.open(new Path(inPath));
            FileOutputStream out = new FileOutputStream(outPath);
            IOUtils.copyBytes(in, out, 1024, true);
            out.close();
        }
        close();
       System.out.println("读取完成！");
       return "下载完成！";
    }

    //上传文件
    @PostMapping("/uploadFile")
    public void uploadFile(String inPath, String outPath) throws Exception {
        connection();
        FileInputStream in = new FileInputStream(inPath);
        FSDataOutputStream out = fs.create(new Path(outPath));
        IOUtils.copyBytes(in, out, 1024, true);
        close();
        System.out.println("上传完成！");
    }

    //删除文件
    @PostMapping("/delete")
    public String delete(String deletePath, boolean recursive) throws Exception {
        connection();
        //递归删除,如果是文件夹,并且文件夹中有文件的话就填写true,否则填false
        boolean b = fs.delete(new Path(deletePath), recursive);
        close();
        return "删除" + deletePath + " " + b;
    }
}
