/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.StringAndBytesText;
import org.elasticsearch.common.text.Text;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.*;

/**
 *
 */
public abstract class StreamInput extends InputStream {

    private Version version = Version.CURRENT;

    public Version getVersion() {
        return this.version;
    }

    public StreamInput setVersion(Version version) {
        this.version = version;
        return this;
    }

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

    public BytesRef readBytesRef() throws IOException {
        int length = readVInt();
        return readBytesRef(length);
    }

    public BytesRef readBytesRef(int length) throws IOException {
        if (length == 0) {
            return new BytesRef();
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return new BytesRef(bytes, 0, length);
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
     * five bytes.  Smaller values take fewer bytes.  Negative numbers
     * will always use all 5 bytes and are therefore better serialized
     * using {@link #readInt}
     */
    public int readVInt() throws IOException {
        byte b = readByte();
        int i = b & 0x7F;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7F) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
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
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 7;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 14;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 21;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 28;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 35;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 42;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        i |= (b & 0x7FL) << 49;
        if ((b & 0x80) == 0) {
            return i;
        }
        b = readByte();
        assert (b & 0x80) == 0;
        return i | ((b & 0x7FL) << 56);
    }

    @Nullable
    public Text readOptionalText() throws IOException {
        int length = readInt();
        if (length == -1) {
            return null;
        }
        return new StringAndBytesText(readBytesReference(length));
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

    @Nullable
    public Integer readOptionalVInt() throws IOException {
        if (readBoolean()) {
            return readVInt();
        }
        return null;
    }

    private final CharsRefBuilder spare = new CharsRefBuilder();

    public String readString() throws IOException {
        final int charCount = readVInt();
        spare.clear();
        spare.grow(charCount);
        int c = 0;
        while (spare.length() < charCount) {
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
                    spare.append((char) c);
                    break;
                case 12:
                case 13:
                    spare.append((char) ((c & 0x1F) << 6 | readByte() & 0x3F));
                    break;
                case 14:
                    spare.append((char) ((c & 0x0F) << 12 | (readByte() & 0x3F) << 6 | (readByte() & 0x3F) << 0));
                    break;
            }
        }
        return spare.toString();
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

    @Nullable
    public final Boolean readOptionalBoolean() throws IOException {
        byte val = readByte();
        if (val == 2) {
            return null;
        }
        if (val == 1) {
            return true;
        }
        return false;
    }

    /**
     * Resets the stream.
     */
    @Override
    public abstract void reset() throws IOException;

    /**
     * Closes the stream to further operations.
     */
    @Override
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
            case 15:
                return readText();
            case 16:
                return readShort();
            case 17:
                return readIntArray();
            case 18:
                return readLongArray();
            case 19:
                return readFloatArray();
            case 20:
                return readDoubleArray();
            default:
                throw new IOException("Can't read unknown type [" + type + "]");
        }
    }

    public int[] readIntArray() throws IOException {
        int length = readVInt();
        int[] values = new int[length];
        for(int i=0; i<length; i++) {
            values[i] = readInt();
        }
        return values;
    }
    
    public long[] readLongArray() throws IOException {
        int length = readVInt();
        long[] values = new long[length];
        for(int i=0; i<length; i++) {
            values[i] = readLong();
        }
        return values;
    }
    
    public float[] readFloatArray() throws IOException {
        int length = readVInt();
        float[] values = new float[length];
        for(int i=0; i<length; i++) {
            values[i] = readFloat();
        }
        return values;
    }
    
    public double[] readDoubleArray() throws IOException {
        int length = readVInt();
        double[] values = new double[length];
        for(int i=0; i<length; i++) {
            values[i] = readDouble();
        }
        return values;
    }

    /**
     * Serializes a potential null value.
     */
    public <T extends Streamable> T readOptionalStreamable(T streamable) throws IOException {
        if (readBoolean()) {
            streamable.readFrom(this);
            return streamable;
        } else {
            return null;
        }
    }

    public <T extends Throwable> T readThrowable() throws IOException {
        try {
            ObjectInputStream oin = new ObjectInputStream(this);
            return (T) oin.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("failed to deserialize exception", e);
        }
    }
}
