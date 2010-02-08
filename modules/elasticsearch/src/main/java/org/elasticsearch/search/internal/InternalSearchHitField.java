/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.search.SearchHitField;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kimchy (Shay Banon)
 */
public class InternalSearchHitField implements SearchHitField {

    private String name;

    private List<Object> values;

    private InternalSearchHitField() {

    }

    public InternalSearchHitField(String name, List<Object> values) {
        this.name = name;
        this.values = values;
    }

    public String name() {
        return name;
    }

    public List<Object> values() {
        return values;
    }

    public static InternalSearchHitField readSearchHitField(DataInput in) throws IOException, ClassNotFoundException {
        InternalSearchHitField result = new InternalSearchHitField();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        name = in.readUTF();
        int size = in.readInt();
        values = new ArrayList<Object>(size);
        for (int i = 0; i < size; i++) {
            Object value;
            byte type = in.readByte();
            if (type == 0) {
                value = in.readUTF();
            } else if (type == 1) {
                value = in.readInt();
            } else if (type == 2) {
                value = in.readLong();
            } else if (type == 3) {
                value = in.readFloat();
            } else if (type == 4) {
                value = in.readDouble();
            } else if (type == 5) {
                value = in.readBoolean();
            } else if (type == 6) {
                int bytesSize = in.readInt();
                value = new byte[bytesSize];
                in.readFully(((byte[]) value));
            } else {
                throw new IOException("Can't read unknown type [" + type + "]");
            }
            values.add(value);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(values.size());
        for (Object obj : values) {
            Class type = obj.getClass();
            if (type == String.class) {
                out.write(0);
                out.writeUTF((String) obj);
            } else if (type == Integer.class) {
                out.write(1);
                out.writeInt((Integer) obj);
            } else if (type == Long.class) {
                out.write(2);
                out.writeLong((Long) obj);
            } else if (type == Float.class) {
                out.write(3);
                out.writeFloat((Float) obj);
            } else if (type == Double.class) {
                out.write(4);
                out.writeDouble((Double) obj);
            } else if (type == Boolean.class) {
                out.write(5);
                out.writeBoolean((Boolean) obj);
            } else if (type == byte[].class) {
                out.write(6);
                out.writeInt(((byte[]) obj).length);
                out.write(((byte[]) obj));
            } else {
                throw new IOException("Can't write type [" + type + "]");
            }
        }
    }
}