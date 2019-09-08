package HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

public class HDFSApp {
    // HDFS文件系统服务器的地址以及端口
    public static final String HDFS_PATH = "hdfs://localhost:8020";
    // HDFS文件系统的操作对象
    FileSystem fileSystem = null;
    // 配置对象
    Configuration configuration = null;

    // 准备资源
    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        // 第一参数是服务器的URI，第二个参数是配置对象，第三个参数是文件系统的用户名
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "mac");
//        System.out.println("~~~~~~~~~~HDFSApp.setUp~~~~~~~~~~");
    }

    // 释放资源
    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
//        System.out.println("~~~~~~~~~~HDFSApp.tearDown~~~~~~~~~~");
    }


    /**
     * 创建HDFS目录
     */
    @Test
    public void mkdir() throws Exception {
        // 需要传递一个Path对象
        fileSystem.mkdirs(new Path("/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        // 创建文件
        FSDataOutputStream outputStream = fileSystem.create(new Path("/test/a.txt"));
        // 写入一些内容到文件中
        outputStream.write("hello hadoop hive HDFS".getBytes());
        outputStream.flush();
        outputStream.close();
    }


    /**
     * 查看HDFS里某个文件的内容
     */
    @Test
    public void cat() throws Exception {
        // 读取文件
        FSDataInputStream in = fileSystem.open(new Path("/test/a.txt"));
        // 将文件内容输出到控制台上，第三个参数表示输出多少字节的内容
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }

    /**
     * 重命名文件
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/test/a.txt");
        Path newPath = new Path("/test/b.txt");
        // 第一个参数是原文件的名称，第二个则是新的名称
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 查看某个目录下所有的文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfs/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("这是一个：" + (fileStatus.isDirectory() ? "文件夹" : "文件"));
            System.out.println("副本系数：" + fileStatus.getReplication());
            System.out.println("大小：" + fileStatus.getLen());
            System.out.println("路径：" + fileStatus.getPath() + "\n");
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void delete() throws Exception {
        // 第二个参数指定是否要递归删除，false=否，true=是
        fileSystem.delete(new Path("/hdfs/a.txt"), false);
    }


    /**
     * 上传本地文件到HDFS
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("/wangqingguo/bigdata/testdata/a.txt");
        Path hdfsPath = new Path("/test/");
        // 第一个参数是本地文件的路径，第二个则是HDFS的路径
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
        System.out.println("上传成功");
    }

    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("/wangqingguo/bigdata/hadoop-2.6.0-cdh5.7.0/");
        Path hdfsPath = new Path("/hdfs/b.txt");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
        System.out.println("下载成功");
    }

}
