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
import java.io.InputStream;
import java.util.*;

/**
 *
 */
public abstract class StreamInput extends InputStream {

    /**
     * Reads and returns a single byte.
     */
    public abstract byte readByte() throws IOException;

    /**
     * Reads a specified number of bytes into an array at the specified offset.
     *
     * @param b      the array to read bytes into
     * @param offset the offset in the array to start storing bytes
     * @param len    the number of bytes to read
     */
    public abstract void readBytes(byte[] b, int offset, int len) throws IOException;

    /**
     * Reads a fresh copy of the bytes.
     */
    public BytesHolder readBytesHolder() throws IOException {
        int size = readVInt();
        if (size == 0) {
            return BytesHolder.EMPTY;
        }
        byte[] bytes = new byte[size];
        readBytes(bytes, 0, size);
        return new BytesHolder(bytes, 0, size);
    }

    /**
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesHolder readBytesReference() throws IOException {
        return readBytesHolder();
    }

    public void readFully(byte[] b) throws IOException {
        readBytes(b, 0, b.length);
    }

    public short readShort() throws IOException {
        return (short) (((readByte() & 0xFF) << 8) | (readByte() & 0xFF));
    }

    /**
     * Reads four bytes and returns an int.
     */
    public int readInt() throws IOException {
        return ((readByte() & 0xFF) << 24) | ((readByte() & 0xFF) << 16)
                | ((readByte() & 0xFF) << 8) | (readByte() & 0xFF);
    }

    /**
     * Reads an int stored in variable-length format.  Reads between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    public int readVInt() throws IOException {
        byte b = readByte();
        int i = b & 0x7F;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 7;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 14;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7F) << 21;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        assert (b & 0x80) == 0;
        return i | ((b & 0x7F) << 28);
    }

    /**
     * Reads eight bytes and returns a long.
     */
    public long readLong() throws IOException {
        return (((long) readInt()) << 32) | (readInt() & 0xFFFFFFFFL);
    }

    /**
     * Reads a long stored in variable-length format.  Reads between one and
     * nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    public long readVLong() throws IOException {
        byte b = readByte();
        long i = b & 0x7FL;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if ((b & 0x80) == 0) return i;
        b = readByte();
        assert (b & 0x80) == 0;
        return i | ((b & 0x7FL) << 56);
    }

    @Nullable
    public String readOptionalUTF() throws IOException {
        if (readBoolean()) {
            return readUTF();
        }
        return null;
    }

    public String readUTF() throws IOException {
        int charCount = readVInt();
        char[] chars = CachedStreamInput.getCharArray(charCount);
        int c, charIndex = 0;
        while (charIndex < charCount) {
            c = readByte() & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    chars[charIndex++] = (char) c;
                    break;
                case 12:
                case 13:
                    chars[charIndex++] = (char) ((c & 0x1F) << 6 | readByte() & 0x3F);
                    break;
                case 14:
                    chars[charIndex++] = (char) ((c & 0x0F) << 12 | (readByte() & 0x3F) << 6 | (readByte() & 0x3F) << 0);
                    break;
            }
        }
        return new String(chars, 0, charCount);
    }


    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    /**
     * Reads a boolean.
     */
    public final boolean readBoolean() throws IOException {
        return readByte() != 0;
    }


    /**
     * Resets the stream.
     */
    public abstract void reset() throws IOException;

    /**
     * Closes the stream to further operations.
     */
    public abstract void close() throws IOException;

//    // IS
//
//    @Override public int read() throws IOException {
//        return readByte();
//    }
//
//    // Here, we assume that we always can read the full byte array
//
//    @Override public int read(byte[] b, int off, int len) throws IOException {
//        readBytes(b, off, len);
//        return len;
//    }

    @Nullable
    public Map<String, Object> readMap() throws IOException {
        return (Map<String, Object>) readGenericValue();
    }

    @SuppressWarnings({"unchecked"})
    @Nullable
    public Object readGenericValue() throws IOException {
        byte type = readByte();
        if (type == -1) {
            return null;
        } else if (type == 0) {
            return readUTF();
        } else if (type == 1) {
            return readInt();
        } else if (type == 2) {
            return readLong();
        } else if (type == 3) {
            return readFloat();
        } else if (type == 4) {
            return readDouble();
        } else if (type == 5) {
            return readBoolean();
        } else if (type == 6) {
            int bytesSize = readVInt();
            byte[] value = new byte[bytesSize];
            readFully(value);
            return value;
        } else if (type == 7) {
            int size = readVInt();
            List list = new ArrayList(size);
            for (int i = 0; i < size; i++) {
                list.add(readGenericValue());
            }
            return list;
        } else if (type == 8) {
            int size = readVInt();
            Object[] list = new Object[size];
            for (int i = 0; i < size; i++) {
                list[i] = readGenericValue();
            }
            return list;
        } else if (type == 9 || type == 10) {
            int size = readVInt();
            Map map;
            if (type == 9) {
                map = new LinkedHashMap(size);
            } else {
                map = new HashMap(size);
            }
            for (int i = 0; i < size; i++) {
                map.put(readUTF(), readGenericValue());
            }
            return map;
        } else if (type == 11) {
            return readByte();
        } else {
            throw new IOException("Can't read unknown type [" + type + "]");
        }
    }
}
