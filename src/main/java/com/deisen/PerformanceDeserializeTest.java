package com.deisen;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import com.deisen.myavro.model.MyUser;
import com.deisen.myavro.model.User;

/***
 * 反串行化的测试
 */
public class PerformanceDeserializeTest {
    private static int MAX_COUNT = 100000;
    private static String FILEPATH_JAVA = "d:/avro/userzhuhj.java";
    private static String FILEPATH_WRITABLE = "d:/avro/userzhuhj.writable";
    private static String FILEPATH_AVRO = "d:/avro/userzhuhj.avro";

    //main
    public static void main(String[] args) {
        System.out.println("------------start---------------------");
        try {
            JavaDeSerizableFile();
            WritableDeSerizableFile();
            AvroDeSeralizeFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("------------end-----------------------");
    }


    /***
     * java反序列化
     */
    private static void JavaDeSerizableFile() throws Exception {
        long l = System.currentTimeMillis();
        FileInputStream fin = new FileInputStream(FILEPATH_JAVA);
        ObjectInputStream oin = new ObjectInputStream(fin);
        for (int i = 0; i < MAX_COUNT; i++) {
            oin.readObject();
        }
        oin.close();
        System.out.println("java deserialize: " + (System.currentTimeMillis() - l));
    }


    /***
     * Writable反序列化
     */
    private static void WritableDeSerizableFile() throws Exception {
        long l = System.currentTimeMillis();
        FileInputStream fin = new FileInputStream(FILEPATH_WRITABLE);
        DataInputStream oos = new DataInputStream(fin);
        MyUser user = new MyUser();
        for (int i = 0; i < MAX_COUNT; i++) {
            user.readFields(oos);
        }
        oos.close();
        System.out.println("writable deserialize: " + (System.currentTimeMillis() - l));
    }

    /***
     *
     * @throws Exception
     */
    private static void AvroDeSeralizeFile() throws Exception {
        long l = System.currentTimeMillis();
        File file = new File(FILEPATH_AVRO);

        // 串行化
        DatumReader<User> datumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> reader = new DataFileReader<User>(file, datumReader);
        User user1 = new User();
        while (reader.hasNext()) {
            reader.next(user1);
//            System.out.println(user1);
        }
        reader.close();
        System.out.println("avro deserialize: " + (System.currentTimeMillis() - l));
    }
}
