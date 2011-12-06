/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class Streamables {

    public static Map<String, Object> readMap(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Object> map = new HashMap<String, Object>(size);
        for (int i = 0; i < size; i++) {
            map.put(in.readUTF(), readMapValue(in));
        }
        return map;
    }

    public static Object readMapValue(StreamInput in) throws IOException {
        byte type = in.readByte();
        if (type == -1) {
            return null;
        } else if (type == 0) {
            return in.readUTF();
        } else if (type == 1) {
            return in.readInt();
        } else if (type == 2) {
            return in.readLong();
        } else if (type == 3) {
            return in.readFloat();
        } else if (type == 4) {
            return in.readDouble();
        } else if (type == 5) {
            return in.readBoolean();
        } else if (type == 6) {
            int bytesSize = in.readVInt();
            byte[] value = new byte[bytesSize];
            in.readFully(value);
            return value;
        } else if (type == 7) {
            int size = in.readVInt();
            List list = new ArrayList(size);
            for (int i = 0; i < size; i++) {
                list.add(readMapValue(in));
            }
            return list;
        } else if (type == 8) {
            int size = in.readVInt();
            Object[] list = new Object[size];
            for (int i = 0; i < size; i++) {
                list[i] = readMapValue(in);
            }
            return list;
        } else if (type == 9) {
            int size = in.readVInt();
            Map map = new HashMap(size);
            for (int i = 0; i < size; i++) {
                map.put(in.readUTF(), readMapValue(in));
            }
            return map;
        } else {
            throw new IOException("Can't read unknown type [" + type + "]");
        }
    }

    public static void writeMap(StreamOutput out, Map<String, Object> map) throws IOException {
        out.writeVInt(map.size());
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            out.writeUTF(entry.getKey());
            writeMapValue(out, entry.getValue());
        }
    }

    private static void writeMapValue(StreamOutput out, Object value) throws IOException {
        if (value == null) {
            out.writeByte((byte) -1);
            return;
        }
        Class type = value.getClass();
        if (type == String.class) {
            out.writeByte((byte) 0);
            out.writeUTF((String) value);
        } else if (type == Integer.class) {
            out.writeByte((byte) 1);
            out.writeInt((Integer) value);
        } else if (type == Long.class) {
            out.writeByte((byte) 2);
            out.writeLong((Long) value);
        } else if (type == Float.class) {
            out.writeByte((byte) 3);
            out.writeFloat((Float) value);
        } else if (type == Double.class) {
            out.writeByte((byte) 4);
            out.writeDouble((Double) value);
        } else if (type == Boolean.class) {
            out.writeByte((byte) 5);
            out.writeBoolean((Boolean) value);
        } else if (type == byte[].class) {
            out.writeByte((byte) 6);
            out.writeVInt(((byte[]) value).length);
            out.writeBytes(((byte[]) value));
        } else if (value instanceof List) {
            out.writeByte((byte) 7);
            List list = (List) value;
            out.writeVInt(list.size());
            for (Object o : list) {
                writeMapValue(out, o);
            }
        } else if (value instanceof Object[]) {
            out.writeByte((byte) 8);
            Object[] list = (Object[]) value;
            out.writeVInt(list.length);
            for (Object o : list) {
                writeMapValue(out, o);
            }
        } else if (value instanceof Map) {
            out.writeByte((byte) 9);
            Map<String, Object> map = (Map<String, Object>) value;
            out.writeVInt(map.size());
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                out.writeUTF(entry.getKey());
                writeMapValue(out, entry.getValue());
            }
        } else {
            throw new IOException("Can't write type [" + type + "]");
        }
    }
}
