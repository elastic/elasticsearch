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

package org.elasticsearch.common.xcontent.xson;

import org.elasticsearch.common.Bytes;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.AbstractXContentParser;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author kimchy (shay.banon)
 */
public class XsonXContentParser extends AbstractXContentParser {

    private final InputStream is;

    private Token currentToken;

    private XsonType xsonType;

    private NumberType currentNumberType;

    private String currentName;

    private Unicode.UTF16Result utf16Result;

    private int valueInt;
    private long valueLong;
    private float valueFloat;
    private double valueDouble;
    private boolean valueBoolean;
    private byte[] valueBytes;

    public XsonXContentParser(InputStream is) throws IOException {
        this.is = is;
        int header = inInt();
        if (header != XsonType.HEADER) {
            throw new IOException("Not xson type header");
        }
    }

    @Override public XContentType contentType() {
        return XContentType.XSON;
    }

    @Override public Token nextToken() throws IOException {
        byte tokenType = (byte) is.read();
        if (tokenType == -1) {
            xsonType = null;
            currentToken = null;
            currentNumberType = null;
            return null;
        } else if (tokenType == XsonType.START_ARRAY.code()) {
            xsonType = XsonType.START_ARRAY;
            currentToken = Token.START_ARRAY;
        } else if (tokenType == XsonType.END_ARRAY.code()) {
            xsonType = XsonType.END_ARRAY;
            currentToken = Token.END_ARRAY;
        } else if (tokenType == XsonType.START_OBJECT.code()) {
            xsonType = XsonType.START_OBJECT;
            currentToken = Token.START_OBJECT;
        } else if (tokenType == XsonType.END_OBJECT.code()) {
            xsonType = XsonType.END_OBJECT;
            currentToken = Token.END_OBJECT;
        } else if (tokenType == XsonType.FIELD_NAME.code()) {
            xsonType = XsonType.FIELD_NAME;
            currentToken = Token.FIELD_NAME;
            // read the field name (interned)
            currentName = inUTF().intern();
        } else if (tokenType == XsonType.VALUE_STRING.code()) {
            xsonType = XsonType.VALUE_STRING;
            currentToken = Token.VALUE_STRING;
            inUtf16();
        } else if (tokenType == XsonType.VALUE_BINARY.code()) {
            xsonType = XsonType.VALUE_BINARY;
            currentToken = Token.VALUE_STRING;
            int length = inVInt();
            valueBytes = new byte[length];
            inBytes(valueBytes, 0, length);
        } else if (tokenType == XsonType.VALUE_INTEGER.code()) {
            xsonType = XsonType.VALUE_INTEGER;
            currentToken = Token.VALUE_NUMBER;
            currentNumberType = NumberType.INT;
            valueInt = inInt();
        } else if (tokenType == XsonType.VALUE_LONG.code()) {
            xsonType = XsonType.VALUE_LONG;
            currentToken = Token.VALUE_NUMBER;
            currentNumberType = NumberType.LONG;
            valueLong = inLong();
        } else if (tokenType == XsonType.VALUE_FLOAT.code()) {
            xsonType = XsonType.VALUE_FLOAT;
            currentToken = Token.VALUE_NUMBER;
            currentNumberType = NumberType.FLOAT;
            valueFloat = inFloat();
        } else if (tokenType == XsonType.VALUE_DOUBLE.code()) {
            xsonType = XsonType.VALUE_DOUBLE;
            currentToken = Token.VALUE_NUMBER;
            currentNumberType = NumberType.DOUBLE;
            valueDouble = inDouble();
        } else if (tokenType == XsonType.VALUE_BOOLEAN.code()) {
            xsonType = XsonType.VALUE_BOOLEAN;
            currentToken = Token.VALUE_BOOLEAN;
            valueBoolean = inBoolean();
        } else if (tokenType == XsonType.VALUE_NULL.code()) {
            xsonType = XsonType.VALUE_NULL;
            currentToken = Token.VALUE_NULL;
        }
        return currentToken;
    }

    @Override public void skipChildren() throws IOException {
        if (xsonType != XsonType.START_OBJECT && xsonType != XsonType.START_ARRAY) {
            return;
        }
        int open = 1;

        /* Since proper matching of start/end markers is handled
         * by nextToken(), we'll just count nesting levels here
         */
        while (true) {
            nextToken();
            if (xsonType == null) {
                return;
            }
            switch (xsonType) {
                case START_OBJECT:
                case START_ARRAY:
                    ++open;
                    break;
                case END_OBJECT:
                case END_ARRAY:
                    if (--open == 0) {
                        return;
                    }
                    break;
            }
        }
    }

    @Override public Token currentToken() {
        return currentToken;
    }

    @Override public String currentName() throws IOException {
        return currentName;
    }

    @Override public String text() throws IOException {
        return new String(utf16Result.result, 0, utf16Result.length);
    }

    @Override public char[] textCharacters() throws IOException {
        return utf16Result.result;
    }

    @Override public int textLength() throws IOException {
        return utf16Result.length;
    }

    @Override public int textOffset() throws IOException {
        return 0;
    }

    @Override public Number numberValue() throws IOException {
        if (currentNumberType == NumberType.INT) {
            return valueInt;
        } else if (currentNumberType == NumberType.LONG) {
            return valueLong;
        } else if (currentNumberType == NumberType.FLOAT) {
            return valueFloat;
        } else if (currentNumberType == NumberType.DOUBLE) {
            return valueDouble;
        }
        throw new IOException("No number type");
    }

    @Override public NumberType numberType() throws IOException {
        return currentNumberType;
    }

    @Override public boolean estimatedNumberType() {
        return false;
    }

    @Override public byte[] binaryValue() throws IOException {
        return valueBytes;
    }

    @Override protected boolean doBooleanValue() throws IOException {
        return valueBoolean;
    }

    @Override protected short doShortValue() throws IOException {
        if (currentNumberType == NumberType.INT) {
            return (short) valueInt;
        } else if (currentNumberType == NumberType.LONG) {
            return (short) valueLong;
        } else if (currentNumberType == NumberType.FLOAT) {
            return (short) valueFloat;
        } else if (currentNumberType == NumberType.DOUBLE) {
            return (short) valueDouble;
        }
        throw new IOException("No number type");
    }

    @Override protected int doIntValue() throws IOException {
        if (currentNumberType == NumberType.INT) {
            return valueInt;
        } else if (currentNumberType == NumberType.LONG) {
            return (int) valueLong;
        } else if (currentNumberType == NumberType.FLOAT) {
            return (int) valueFloat;
        } else if (currentNumberType == NumberType.DOUBLE) {
            return (int) valueDouble;
        }
        throw new IOException("No number type");
    }

    @Override protected long doLongValue() throws IOException {
        if (currentNumberType == NumberType.LONG) {
            return valueLong;
        } else if (currentNumberType == NumberType.INT) {
            return (long) valueInt;
        } else if (currentNumberType == NumberType.FLOAT) {
            return (long) valueFloat;
        } else if (currentNumberType == NumberType.DOUBLE) {
            return (long) valueDouble;
        }
        throw new IOException("No number type");
    }

    @Override protected float doFloatValue() throws IOException {
        if (currentNumberType == NumberType.FLOAT) {
            return valueFloat;
        } else if (currentNumberType == NumberType.INT) {
            return (float) valueInt;
        } else if (currentNumberType == NumberType.LONG) {
            return (float) valueLong;
        } else if (currentNumberType == NumberType.DOUBLE) {
            return (float) valueDouble;
        }
        throw new IOException("No number type");
    }

    @Override protected double doDoubleValue() throws IOException {
        if (currentNumberType == NumberType.DOUBLE) {
            return valueDouble;
        } else if (currentNumberType == NumberType.INT) {
            return (double) valueInt;
        } else if (currentNumberType == NumberType.FLOAT) {
            return (double) valueFloat;
        } else if (currentNumberType == NumberType.LONG) {
            return (double) valueLong;
        }
        throw new IOException("No number type");
    }

    @Override public void close() {
        try {
            is.close();
        } catch (IOException e) {
            // ignore
        }
    }

    private short inShort() throws IOException {
        return (short) (((is.read() & 0xFF) << 8) | (is.read() & 0xFF));
    }

    /**
     * Reads four bytes and returns an int.
     */
    private int inInt() throws IOException {
        return ((is.read() & 0xFF) << 24) | ((is.read() & 0xFF) << 16)
                | ((is.read() & 0xFF) << 8) | (is.read() & 0xFF);
    }

    /**
     * Reads an int stored in variable-length format.  Reads between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    private int inVInt() throws IOException {
        int b = is.read();
        int i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = is.read();
            i |= (b & 0x7F) << shift;
        }
        return i;
    }

    /**
     * Reads eight bytes and returns a long.
     */
    private long inLong() throws IOException {
        return (((long) inInt()) << 32) | (inInt() & 0xFFFFFFFFL);
    }

    /**
     * Reads a long stored in variable-length format.  Reads between one and
     * nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    private long readVLong() throws IOException {
        int b = is.read();
        long i = b & 0x7F;
        for (int shift = 7; (b & 0x80) != 0; shift += 7) {
            b = is.read();
            i |= (b & 0x7FL) << shift;
        }
        return i;
    }

    private String inUTF() throws IOException {
        inUtf16();
        return new String(utf16Result.result, 0, utf16Result.length);
    }

    /**
     * Reads a string.
     */
    private void inUtf16() throws IOException {
        int length = inVInt();
        byte[] bytes = Bytes.cachedBytes.get().get();
        if (bytes == null || length > bytes.length) {
            bytes = new byte[(int) (length * 1.25)];
            Bytes.cachedBytes.get().set(bytes);
        }
        inBytes(bytes, 0, length);
        utf16Result = Unicode.fromBytesAsUtf16(bytes, 0, length);
    }

    private float inFloat() throws IOException {
        return Float.intBitsToFloat(inInt());
    }

    private double inDouble() throws IOException {
        return Double.longBitsToDouble(inLong());
    }

    /**
     * Reads a boolean.
     */
    private boolean inBoolean() throws IOException {
        byte ch = (byte) is.read();
        if (ch < 0)
            throw new EOFException();
        return (ch != 0);
    }

    private void inBytes(byte[] b, int offset, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = is.read(b, offset + n, len - n);
            if (count < 0)
                throw new EOFException();
            n += count;
        }
    }

}
