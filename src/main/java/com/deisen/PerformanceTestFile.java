package com.deisen;

import com.deisen.myavro.model.MyUser;
import com.deisen.myavro.model.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

/***
 * 性能评测
 * psvm :main
 * fori :
 * sout :
 */
public class PerformanceTestFile {
    //ctrl+shift+U
    private static int MAX_COUNT = 100000;
    private static String FILEPATH_JAVA = "d:/avro/userzhuhj.java";
    private static String FILEPATH_WRITABLE = "d:/avro/userzhuhj.writable";
    private static String FILEPATH_AVRO = "d:/avro/userzhuhj.avro";

    //主函数
    public static void main(String[] args) throws IOException {
        System.out.println("------------start---------------------");
        JavaSerizableFile();
        WritableSerizableFile();
        AvroSeralizeFile();
        System.out.println("------------over---------------------");
    }

    /***
     * java序列化
     */
    private static void JavaSerizableFile() throws IOException {
        long l = System.currentTimeMillis();
        FileOutputStream baos = new FileOutputStream(FILEPATH_JAVA);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        MyUser user;
        for (int i = 0; i < MAX_COUNT; i++) {
            user = new MyUser();
            user.setName("zhuhj" + i);
            user.setFavorite_color("yellow" + i);
            user.setFavorite_number(666 + i);
            oos.writeObject(user);
        }
        oos.close();
        System.out.println("java serialize: " + (System.currentTimeMillis() - l));
    }

    /***
     * Writable序列化
     */
    private static void WritableSerizableFile() throws IOException {
        long l = System.currentTimeMillis();
        FileOutputStream baos = new FileOutputStream(FILEPATH_WRITABLE);
        DataOutputStream oos = new DataOutputStream(baos);
        MyUser myuser;
        for (int i = 0; i < MAX_COUNT; i++) {
            myuser = new MyUser("zhuhj" + i, 666 + i, "yellow" + i);
            myuser.write(oos);
        }
        oos.close();
        System.out.println("writable serialize: " + (System.currentTimeMillis() - l) );
    }

    private static void AvroSeralizeFile() throws IOException {
        long l = System.currentTimeMillis();
        FileOutputStream baos = new FileOutputStream(FILEPATH_AVRO);

        // 串行化
        SpecificDatumWriter<User> datumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> writer = new DataFileWriter<User>(datumWriter);
        User user1 = new User();
        writer.create(user1.getSchema(), baos);
        User user;
        for (int i = 0; i < MAX_COUNT; i++) {
            user = new User();
            user.setName("zhuhj" + i);
            user.setFavoriteNumber(666 + i);
            user.setFavoriteColor("yellow" + i);
            writer.append(user);
        }
        writer.close();
        System.out.println("avro serialize: " + (System.currentTimeMillis() - l) );
    }
}
