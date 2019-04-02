package com.deisen;

import com.deisen.myavro.model.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App 
{
    @SuppressWarnings("resource")
    public static void main(String[] args) throws IOException {
        User user1 = new User();
        user1.setName("zhuhj");
        user1.setFavoriteColor("red");
        user1.setFavoriteNumber(666);
        // 构造函数
        User user2 = new User("zhuzx", 777, "blue");
        // builder
        User user3 = User.newBuilder()//
                .setFavoriteColor("black")//
                .setFavoriteNumber(888)//
                .setName("zhuxy")//
                .build();
        // 串行化
        SpecificDatumWriter<User> datumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> writer = new DataFileWriter<>(datumWriter);
        //avro数据文件
        writer.create(user1.getSchema(), new File("d:/avro/user.avro"));
        writer.append(user1);
        writer.append(user2);
        writer.append(user3);
        writer.close();
        System.out.println("Hello World!");
        System.out.println("over------------------------------");
    }
}
