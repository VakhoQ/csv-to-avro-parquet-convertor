package com.bigdata.avro;

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
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;

import org.apache.hadoop.fs.FileSystem;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class test {

    public static void main(String[] args) throws Exception {

//          new test().init2();
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "172.16.68.131:8020");

        Path p=new Path("hdfs://172.16.68.131:8020/user/hdfs/test_file.txt");
        FileSystem fs = FileSystem.get(conf);
        fs.create(p);
        System.out.println(p.getName() + " exists: " + fs.exists(p));


//        String uri = "hdfs://172.16.68.131:9000/hello.html";
//        Configuration config = new Configuration();
//        config.set("fs.default.name", "hdfs://localhost:9000");
//
//        FileSystem fs = FileSystem.get(URI.create(uri), config);
//
//        try {
//
//
//            FSDataOutputStream fin = fs.create(new Path(uri));
//            fin.writeUTF("hello");
//            fin.close();
//        }catch (Exception e){
//            e.printStackTrace();
//        }



    }

    private void init2() {

        Path path =   new Path("172.16.68.131:9000/test/out/test.parquet");

        ParquetWriter<GenericData.Record> writer = null;

        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(Destination.getClassSchema())
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();


            File theFile = new File("/home/vq/Downloads/expedia-hotel-recommendations/destinations.csv");
            LineIterator it = FileUtils.lineIterator(theFile, "UTF-8");
            String header = it.nextLine();
            String [] headerLines = header.split(",");

            try {
                while (it.hasNext()) {
                    String line = it.nextLine();
                    String [] lineComponents = line.split(",");
                    GenericData.Record record = new GenericData.Record(Destination.getClassSchema());
                    for (int i = 0; i < lineComponents.length; i++) {
                        if (i == 0) {
                            record.put(headerLines[i],  new Integer(lineComponents[i]));
                        } else{
                            record.put(headerLines[i],  new Float(lineComponents[i]));
                        }
                    }
                    writer.write(record);
                }
            } finally {
                it.close();
            }


        }catch(Exception e) {
            e.printStackTrace();
        }finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
//
//
    private  void init() throws Exception {



        String baseFolder = getBaseDir();
        File theFile = new File("/home/vq/Downloads/expedia-hotel-recommendations/destinations.csv");

        DatumWriter<Destination> userDatumWriter = new SpecificDatumWriter<>(Destination.class);
        DataFileWriter<Destination> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create( Destination.getClassSchema(), new File("/home/vq/test/dest.schema"));



        LineIterator it = FileUtils.lineIterator(theFile, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                String [] lineComponents = line.split(",");
                if("srch_destination_id".equalsIgnoreCase(lineComponents[0])) continue;
                Destination dest = new Destination();
                for (int i = 0; i < lineComponents.length; i++) {
                    if (i == 0) dest.put(i, new Integer(lineComponents[i]));
                    else dest.put(i, new Float(lineComponents[i]));
                }
                dataFileWriter.append(dest);
            }
        } finally {
            it.close();
        }
        dataFileWriter.flush();
        dataFileWriter.close();


        System.out.println("READ AVRO FILE");
        File file = new File("/home/vq/test/dest.schema");
        DatumReader<Destination> userDatumReader = new SpecificDatumReader<Destination>(Destination.class);
        DataFileReader<Destination> dataFileReader = new DataFileReader<Destination>(file, userDatumReader);



        Destination user = null;

        System.out.println(Destination.getClassSchema().getFields());
        while (dataFileReader.hasNext()) {
            user = dataFileReader.next(user);
            System.out.println(user);
        }








    }



    private String getBaseDir() throws URISyntaxException {
        URL res = getClass().getClassLoader().getResource("csv");
        File file = Paths.get(res.toURI()).toFile();
        return file.getAbsolutePath();
    }


}
