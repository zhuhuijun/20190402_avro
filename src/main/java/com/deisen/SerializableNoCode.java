package com.deisen;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;

public class SerializableNoCode {
    public static void main(String[] args) throws Exception {
        String filepath = "d:/avro/user.avsc";
        // 加载Schema对象
        Schema schema = new Schema.Parser().parse(new File(filepath));
        // 常规记录类
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "linqq");
        user1.put("favorite_number", 666);
        user1.put("favorite_color", "yellow");

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "linqq2");
        user2.put("favorite_number", 666);
        user2.put("favorite_color", "yellow2");

        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<GenericRecord>(datumWriter);
        // avro数据文件
        writer.create(schema, new File("d:/avro/user2.avro"));
        writer.append(user1);
        writer.append(user2);
        writer.close();
        System.out.println("Hello World!");
        System.out.println("over---------------------------");
    }
}
