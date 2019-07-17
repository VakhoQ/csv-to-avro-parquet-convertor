import com.bigdata.avro.schema.Destination;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class HdfsUploaderTest {

    public static void main(String[] args) throws Exception {
        uploadToHdfs(args[0], args[1], args[2], args[3]);
    }

    /**
     *
     * @param inputPath   Example: "/home/vq/hadoop/Expedia-Hotel-Recoomendations/sample_submission.csv"
     * @param outputPath  Example: "hdfs://127.0.0.1:8020/user/maria_dev/sample_submission.csv"
     * @param userName    Example: "maria_dev", "hdfs" etc
     * @param nameNode   Example: "hdfs://127.0.0.1:8020"
     * @throws IOException
     */
    private static void uploadToHdfs(String inputPath, String outputPath, String userName, String nameNode ) throws IOException {

        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", userName);
        conf.set("fs.defaultFS", nameNode);
        conf.set("dfs.datanode.max.xcievers", "4096");

        FileSystem fs = FileSystem.get(conf);
        OutputStream os = fs.create(new Path(outputPath));
        InputStream is = new BufferedInputStream(new FileInputStream(inputPath));
        IOUtils.copyBytes(is, os, conf);
    }


}
