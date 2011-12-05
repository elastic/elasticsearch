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

package org.elasticsearch.common.io.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

/**
 * @author kimchy (shay.banon)
 */
public abstract class StreamInput extends InputStream {

    /**
     * working arrays initialized on demand by readUTF
     */
    private byte bytearr[] = new byte[80];
    protected char chararr[] = new char[80];

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

    // COPIED from DataInputStream

    public String readUTF() throws IOException {
        int utflen = readInt();
        if (utflen == 0) {
            return "";
        }
        if (bytearr.length < utflen) {
            bytearr = new byte[utflen * 2];
            chararr = new char[utflen * 2];
        }
        char[] chararr = this.chararr;
        byte[] bytearr = this.bytearr;

        int c, char2, char3;
        int count = 0;
        int chararr_count = 0;

        readBytes(bytearr, 0, utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) break;
            count++;
            chararr[chararr_count++] = (char) c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    /* 0xxxxxxx*/
                    count++;
                    chararr[chararr_count++] = (char) c;
                    break;
                case 12:
                case 13:
                    /* 110x xxxx   10xx xxxx*/
                    count += 2;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    char2 = (int) bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80)
                        throw new UTFDataFormatException(
                                "malformed input around byte " + count);
                    chararr[chararr_count++] = (char) (((c & 0x1F) << 6) |
                            (char2 & 0x3F));
                    break;
                case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
                    count += 3;
                    if (count > utflen)
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    char2 = (int) bytearr[count - 2];
                    char3 = (int) bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count - 1));
                    chararr[chararr_count++] = (char) (((c & 0x0F) << 12) |
                            ((char2 & 0x3F) << 6) |
                            ((char3 & 0x3F) << 0));
                    break;
                default:
                    /* 10xx xxxx,  1111 xxxx */
                    throw new UTFDataFormatException(
                            "malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararr_count);
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
        byte ch = readByte();
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
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
}
