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

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class StreamOutput extends OutputStream {

    /**
     * Writes a single byte.
     */
    public abstract void writeByte(byte b) throws IOException;

    /**
     * Writes an array of bytes.
     *
     * @param b the bytes to write
     */
    public void writeBytes(byte[] b) throws IOException {
        writeBytes(b, 0, b.length);
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param length the number of bytes to write
     */
    public void writeBytes(byte[] b, int length) throws IOException {
        writeBytes(b, 0, length);
    }

    /**
     * Writes an array of bytes.
     *
     * @param b      the bytes to write
     * @param offset the offset in the byte array
     * @param length the number of bytes to write
     */
    public abstract void writeBytes(byte[] b, int offset, int length) throws IOException;

    public void writeBytesHolder(byte[] bytes, int offset, int length) throws IOException {
        writeVInt(length);
        writeBytes(bytes, offset, length);
    }

    public void writeBytesHolder(@Nullable BytesHolder bytes) throws IOException {
        if (bytes == null) {
            writeVInt(0);
        } else {
            writeVInt(bytes.length());
            writeBytes(bytes.bytes(), bytes.offset(), bytes.length());
        }
    }

    public final void writeShort(short v) throws IOException {
        writeByte((byte) (v >> 8));
        writeByte((byte) v);
    }

    /**
     * Writes an int as four bytes.
     */
    public void writeInt(int i) throws IOException {
        writeByte((byte) (i >> 24));
        writeByte((byte) (i >> 16));
        writeByte((byte) (i >> 8));
        writeByte((byte) i);
    }

    /**
     * Writes an int in a variable-length format.  Writes between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    public void writeVInt(int i) throws IOException {
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    /**
     * Writes a long as eight bytes.
     */
    public void writeLong(long i) throws IOException {
        writeInt((int) (i >> 32));
        writeInt((int) i);
    }

    /**
     * Writes an long in a variable-length format.  Writes between one and five
     * bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    public void writeVLong(long i) throws IOException {
        while ((i & ~0x7F) != 0) {
            writeByte((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        writeByte((byte) i);
    }

    public void writeOptionalUTF(@Nullable String str) throws IOException {
        if (str == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeUTF(str);
        }
    }

    /**
     * Writes a string.
     */
    public void writeUTF(String str) throws IOException {
        int charCount = str.length();
        writeVInt(charCount);
        int c;
        for (int i = 0; i < charCount; i++) {
            c = str.charAt(i);
            if (c <= 0x007F) {
                writeByte((byte) c);
            } else if (c > 0x07FF) {
                writeByte((byte) (0xE0 | c >> 12 & 0x0F));
                writeByte((byte) (0x80 | c >> 6 & 0x3F));
                writeByte((byte) (0x80 | c >> 0 & 0x3F));
            } else {
                writeByte((byte) (0xC0 | c >> 6 & 0x1F));
                writeByte((byte) (0x80 | c >> 0 & 0x3F));
            }
        }
    }

    public void writeFloat(float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    public void writeDouble(double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }


    private static byte ZERO = 0;
    private static byte ONE = 1;

    /**
     * Writes a boolean.
     */
    public void writeBoolean(boolean b) throws IOException {
        writeByte(b ? ONE : ZERO);
    }

    /**
     * Forces any buffered output to be written.
     */
    public abstract void flush() throws IOException;

    /**
     * Closes this stream to further operations.
     */
    public abstract void close() throws IOException;

    public abstract void reset() throws IOException;

    @Override
    public void write(int b) throws IOException {
        writeByte((byte) b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        writeBytes(b, off, len);
    }

    public void writeMap(@Nullable Map<String, Object> map) throws IOException {
        writeGenericValue(map);
    }

    public void writeGenericValue(@Nullable Object value) throws IOException {
        if (value == null) {
            writeByte((byte) -1);
            return;
        }
        Class type = value.getClass();
        if (type == String.class) {
            writeByte((byte) 0);
            writeUTF((String) value);
        } else if (type == Integer.class) {
            writeByte((byte) 1);
            writeInt((Integer) value);
        } else if (type == Long.class) {
            writeByte((byte) 2);
            writeLong((Long) value);
        } else if (type == Float.class) {
            writeByte((byte) 3);
            writeFloat((Float) value);
        } else if (type == Double.class) {
            writeByte((byte) 4);
            writeDouble((Double) value);
        } else if (type == Boolean.class) {
            writeByte((byte) 5);
            writeBoolean((Boolean) value);
        } else if (type == byte[].class) {
            writeByte((byte) 6);
            writeVInt(((byte[]) value).length);
            writeBytes(((byte[]) value));
        } else if (value instanceof List) {
            writeByte((byte) 7);
            List list = (List) value;
            writeVInt(list.size());
            for (Object o : list) {
                writeGenericValue(o);
            }
        } else if (value instanceof Object[]) {
            writeByte((byte) 8);
            Object[] list = (Object[]) value;
            writeVInt(list.length);
            for (Object o : list) {
                writeGenericValue(o);
            }
        } else if (value instanceof Map) {
            if (value instanceof LinkedHashMap) {
                writeByte((byte) 9);
            } else {
                writeByte((byte) 10);
            }
            Map<String, Object> map = (Map<String, Object>) value;
            writeVInt(map.size());
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                writeUTF(entry.getKey());
                writeGenericValue(entry.getValue());
            }
        } else if (type == Byte.class) {
            writeByte((byte) 11);
            writeByte((Byte) value);
        } else {
            throw new IOException("Can't write type [" + type + "]");
        }
    }
}
