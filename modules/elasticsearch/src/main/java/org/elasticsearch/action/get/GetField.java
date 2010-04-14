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

package org.elasticsearch.action.get;

import org.elasticsearch.util.io.stream.StreamInput;
import org.elasticsearch.util.io.stream.StreamOutput;
import org.elasticsearch.util.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class GetField implements Streamable, Iterable<Object> {

    private String name;

    private List<Object> values;

    private GetField() {

    }

    public GetField(String name, List<Object> values) {
        this.name = name;
        this.values = values;
    }

    public String name() {
        return name;
    }

    public String getName() {
        return name;
    }

    public List<Object> values() {
        return values;
    }

    public List<Object> getValues() {
        return values;
    }

    @Override public Iterator<Object> iterator() {
        return values.iterator();
    }

    public static GetField readGetField(StreamInput in) throws IOException {
        GetField result = new GetField();
        result.readFrom(in);
        return result;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        int size = in.readVInt();
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
                int bytesSize = in.readVInt();
                value = new byte[bytesSize];
                in.readFully(((byte[]) value));
            } else {
                throw new IOException("Can't read unknown type [" + type + "]");
            }
            values.add(value);
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeVInt(values.size());
        for (Object obj : values) {
            Class type = obj.getClass();
            if (type == String.class) {
                out.writeByte((byte) 0);
                out.writeUTF((String) obj);
            } else if (type == Integer.class) {
                out.writeByte((byte) 1);
                out.writeInt((Integer) obj);
            } else if (type == Long.class) {
                out.writeByte((byte) 2);
                out.writeLong((Long) obj);
            } else if (type == Float.class) {
                out.writeByte((byte) 3);
                out.writeFloat((Float) obj);
            } else if (type == Double.class) {
                out.writeByte((byte) 4);
                out.writeDouble((Double) obj);
            } else if (type == Boolean.class) {
                out.writeByte((byte) 5);
                out.writeBoolean((Boolean) obj);
            } else if (type == byte[].class) {
                out.writeByte((byte) 6);
                out.writeVInt(((byte[]) obj).length);
                out.writeBytes(((byte[]) obj));
            } else {
                throw new IOException("Can't write type [" + type + "]");
            }
        }
    }
}
