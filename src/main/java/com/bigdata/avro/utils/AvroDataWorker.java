package com.bigdata.avro.utils;

import com.bigdata.avro.schema.Destination;
import com.google.common.collect.Streams;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class AvroDataWorker {

    static Logger logger = Logger.getLogger(ParquetDataWorker.class.getName());


    public static <T extends SpecificRecordBase> void csvToAvro(String inputPath, String outputPath, Schema schema, Class<T> clazz) throws Exception {

        logger.log(Level.INFO, "csv to schema converting started");

        DatumWriter<T> userDatumWriter = new SpecificDatumWriter<>(clazz);
        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(schema, new File(outputPath));
        LineIterator it = FileUtils.lineIterator(new File(inputPath), "UTF-8");
        String header = it.nextLine();
        String[] headerComp = header.split(",");

        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                String[] lineComponents = line.split(",");
                T dest = clazz.newInstance();
                for (int i = 0; i < lineComponents.length; i++) {
                    if (schema.getField(headerComp[i]).schema().getType() == Schema.Type.INT) {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            dest.put(i, 0);
                        }else{
                            dest.put(i, Integer.parseInt(lineComponents[i]));
                        }
                    } else if (schema.getField(headerComp[i]).schema().getType() == Schema.Type.FLOAT) {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            dest.put(i, 0.0f);
                        }else{
                            dest.put(i, Float.parseFloat(lineComponents[i]));
                        }
                    } else {
                        if(StringUtils.isAllEmpty(lineComponents[i])){
                            dest.put(i, "");
                        }else{
                            dest.put(i, lineComponents[i]);
                        }
                    }
                }
                dataFileWriter.append(dest);
            }
        } finally {
            it.close();
        }
        dataFileWriter.flush();
        dataFileWriter.close();

    }

    private static Object getLineComponent(String obj) {


//        logger.log(Level.INFO, "converting value " + obj);
        try {
            return Integer.parseInt(obj);
        } catch (Exception e) {
            logger.log(Level.FINE, "data is not integer");
        }

        try {
            return Float.parseFloat(obj);
        } catch (Exception e) {
            logger.log(Level.FINE, "data is not float");
        }


        return obj;
    }

    public static <T extends SpecificRecordBase> List<T> avroToObj(File input, Class<T> clazz) throws Exception {
        DatumReader<T> userDatumReader = new SpecificDatumReader<>(clazz);
        DataFileReader<T> dataFileReader = new DataFileReader<>(input, userDatumReader);
        return Streams.stream(dataFileReader.iterator()).collect(Collectors.toList());
    }

    /**
     * Converts JSON to Generic object
     *
     * @param json   json in String
     * @param schema Schema object
     * @return Generic record
     * @throws Exception if we cant parse json and can't convert to Generic record
     */
    public static GenericRecord fromJsonToAvro(String json, Schema schema) throws Exception {

        try {
            InputStream input = new ByteArrayInputStream(json.getBytes());
            DataInputStream din = new DataInputStream(input);

            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);

            DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
            Object datum = reader.read(null, decoder);

            GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(schema);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

            Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            w.write(datum, encoder);
            encoder.flush();

            DatumReader<GenericRecord> genericRecordDatumReader = new GenericDatumReader<>(schema);
            Decoder binaryDecoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
            logger.log(Level.INFO, "converted from json to schema");
            return genericRecordDatumReader.read(null, binaryDecoder);
        } catch (Exception e) {
            throw new Exception("can't convert json to schema record", e);
        }
    }


}
