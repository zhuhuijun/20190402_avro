package com.deisen;

import com.deisen.myavro.model.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/***
 * 性能评测
 * psvm :main
 * fori :
 * sout :
 */
public class PerformanceTest {
    //ctrl+shift+U
    private static int MAX_COUNT = 10000;

    //主函数
    public static void main(String[] args) throws IOException {
        System.out.println("------------start---------------------");
        JavaSerizable();
        WritableSerizable();
        AvroSeralize();
        System.out.println("------------over---------------------");
    }

    /***
     * java序列化
     */
    private static  void JavaSerizable() throws IOException {
        long l = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        for (int i = 0; i < MAX_COUNT; i++) {
            User user = new User();
            user.setName("zhuhj" + i);
            user.setFavoriteColor("yellow" + i);
            user.setFavoriteNumber(666 + i);
            oos.writeObject(user);
        }
        oos.close();
        System.out.println("java serialize: " + (System.currentTimeMillis() - l) + " length:" + baos.toByteArray().length);
    }

    /***
     * Writable序列化
     */
    private static void WritableSerizable() throws IOException {
        long l = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream oos = new DataOutputStream(baos);
        for (int i = 0; i < MAX_COUNT; i++) {
            oos.writeUTF("zhuhj" + i);
            oos.writeInt(666 + i);
            oos.writeUTF("yellow" + i);
        }
        oos.close();
        System.out.println("writable serialize: " + (System.currentTimeMillis() - l) + " length:" + baos.toByteArray().length);
    }

    private static void AvroSeralize() throws IOException {
        long l = System.currentTimeMillis();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        // 串行化
        SpecificDatumWriter<User> datumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> writer = new DataFileWriter<User>(datumWriter);
        User user1 = new User();
        writer.create(user1.getSchema(), baos);
        for (int i = 0; i < MAX_COUNT; i++) {
            User user = new User();
            user.setName("zhuhj"+i);
            user.setFavoriteNumber(666+i);
            user.setFavoriteColor("yellow"+i);
            writer.append(user);
        }
        writer.close();
        System.out.println("avro serialize: " + (System.currentTimeMillis() - l) + " length:" + baos.toByteArray().length);
    }
}
