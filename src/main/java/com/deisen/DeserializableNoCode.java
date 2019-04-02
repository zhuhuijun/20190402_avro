package com.deisen;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

public class DeserializableNoCode {
    public static void main(String[] args) throws Exception {
        File file = new File("d:/avro/user2.avro");
        String filepath = "d:/avro/user.avsc";
        // 加载Schema对象
        Schema schema = new Schema.Parser().parse(new File(filepath));
        DatumReader<GenericRecord> userReader = new SpecificDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader;
        try {
            dataFileReader = new DataFileReader<GenericRecord>(file, userReader);
            GenericRecord user = null;

            while (dataFileReader.hasNext()) {
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
