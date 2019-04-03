package com.deisen.myavro.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class MyUser implements Writable, Serializable {
    private static final long serialVersionUID = -1536289812397141151L;
    private String name;
    private Integer favorite_number;
    private String favorite_color;

    public MyUser() {
    }

    public MyUser(String name, Integer favorite_number, String favorite_color) {
        this.name = name;
        this.favorite_number = favorite_number;
        this.favorite_color = favorite_color;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getFavorite_number() {
        return favorite_number;
    }

    public void setFavorite_number(Integer favorite_number) {
        this.favorite_number = favorite_number;
    }

    public String getFavorite_color() {
        return favorite_color;
    }

    public void setFavorite_color(String favorite_color) {
        this.favorite_color = favorite_color;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);
        dataOutput.writeInt(favorite_number);
        dataOutput.writeUTF(favorite_color);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.favorite_number = dataInput.readInt();
        this.favorite_color = dataInput.readUTF();
    }
}
