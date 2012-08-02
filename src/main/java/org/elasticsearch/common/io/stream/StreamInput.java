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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.joda.time.DateTime;

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
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference() throws IOException {
        int length = readVInt();
        return readBytesReference(length);
    }

    /**
     * Reads a bytes reference from this stream, might hold an actual reference to the underlying
     * bytes of the stream.
     */
    public BytesReference readBytesReference(int length) throws IOException {
        if (length == 0) {
            return BytesArray.EMPTY;
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return new BytesArray(bytes, 0, length);
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

    /**
     * @deprecated use {@link #readOptionalString()}
     */
    @Nullable
    @Deprecated
    public String readOptionalUTF() throws IOException {
        if (readBoolean()) {
            return readUTF();
        }
        return null;
    }

    public Text readText() throws IOException {
        // use StringAndBytes so we can cache the string if its ever converted to it
        int length = readInt();
        return new StringAndBytesText(readBytesReference(length));
    }

    @Nullable
    public String readOptionalString() throws IOException {
        if (readBoolean()) {
            return readString();
        }
        return null;
    }

    public String readString() throws IOException {
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

    /**
     * @deprecated use {@link #readString()}
     */
    @Deprecated
    public String readUTF() throws IOException {
        return readString();
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

    public String[] readStringArray() throws IOException {
        int size = readVInt();
        if (size == 0) {
            return Strings.EMPTY_ARRAY;
        }
        String[] ret = new String[size];
        for (int i = 0; i < size; i++) {
            ret[i] = readString();
        }
        return ret;
    }

    @Nullable
    public Map<String, Object> readMap() throws IOException {
        return (Map<String, Object>) readGenericValue();
    }

    @SuppressWarnings({"unchecked"})
    @Nullable
    public Object readGenericValue() throws IOException {
        byte type = readByte();
        switch (type) {
            case -1:
                return null;
            case 0:
                return readString();
            case 1:
                return readInt();
            case 2:
                return readLong();
            case 3:
                return readFloat();
            case 4:
                return readDouble();
            case 5:
                return readBoolean();
            case 6:
                int bytesSize = readVInt();
                byte[] value = new byte[bytesSize];
                readBytes(value, 0, bytesSize);
                return value;
            case 7:
                int size = readVInt();
                List list = new ArrayList(size);
                for (int i = 0; i < size; i++) {
                    list.add(readGenericValue());
                }
                return list;
            case 8:
                int size8 = readVInt();
                Object[] list8 = new Object[size8];
                for (int i = 0; i < size8; i++) {
                    list8[i] = readGenericValue();
                }
                return list8;
            case 9:
                int size9 = readVInt();
                Map map9 = new LinkedHashMap(size9);
                for (int i = 0; i < size9; i++) {
                    map9.put(readString(), readGenericValue());
                }
                return map9;
            case 10:
                int size10 = readVInt();
                Map map10 = new HashMap(size10);
                for (int i = 0; i < size10; i++) {
                    map10.put(readString(), readGenericValue());
                }
                return map10;
            case 11:
                return readByte();
            case 12:
                return new Date(readLong());
            case 13:
                return new DateTime(readLong());
            case 14:
                return readBytesReference();
            default:
                throw new IOException("Can't read unknown type [" + type + "]");
        }
    }
}
