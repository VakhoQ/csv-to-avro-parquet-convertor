import com.bigdata.exceptions.HdfsException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;

public class HdfsUploaderTest {

    private static final Logger logger = LogManager.getLogger(HdfsUploaderTest.class.getName());

    public static void main(String[] args) throws HdfsException{

        if(args.length!=4){
            logger.warn("Input params are not valid! Correct param sample: \n" +
                    "1. INPUT_PATH \n" +
                    "2. hdfs://IP:NAME_NODE_PORT/PATTH \n" +
                    "3. HADOOP_USER \n" +
                    "4. NAME_NODE_IP_PORT  ");
            logger.error("expecting the following 4 input params: inputPath, outputPath, userName, nameNode ");
            return;
        }
        uploadToHdfs(args[0], args[1], args[2], args[3]);
    }

    /**
     *
     * @param inputPath   input local file path
     * @param outputPath  out path in hdfs
     * @param userName    username
     * @param nameNode   namenode ip:port
     * @throws IOException
     */
    private static void uploadToHdfs(String inputPath, String outputPath, String userName, String nameNode ) throws HdfsException {

        try {
            Configuration conf = new Configuration();
            System.setProperty("HADOOP_USER_NAME", userName);
            conf.set("fs.defaultFS", nameNode);
            conf.set("dfs.datanode.max.xcievers", "4096");

            FileSystem fs = FileSystem.get(conf);
            OutputStream os = fs.create(new Path(outputPath));
            InputStream is = new BufferedInputStream(new FileInputStream(inputPath));
            IOUtils.copyBytes(is, os, conf);
        }catch (Exception e ){
            logger.error("cant upload data to HDFS" , e);
            throw  new HdfsException("cant upload data to HDFS" , e);
        }


    }


}
