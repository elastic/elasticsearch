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

package org.elasticsearch.util.xcontent.xson;

import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.util.Unicode;
import org.elasticsearch.util.xcontent.XContentGenerator;
import org.elasticsearch.util.xcontent.XContentType;
import org.elasticsearch.util.xcontent.support.AbstractXContentGenerator;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author kimchy (shay.banon)
 */
public class XsonXContentGenerator extends AbstractXContentGenerator implements XContentGenerator {

    private final OutputStream out;

    public XsonXContentGenerator(OutputStream out) throws IOException {
        this.out = out;
        outInt(XsonType.HEADER);
    }

    @Override public XContentType contentType() {
        return XContentType.XSON;
    }

    @Override public void usePrettyPrint() {
        // irrelevant
    }

    @Override public void writeStartArray() throws IOException {
        out.write(XsonType.START_ARRAY.code());
    }

    @Override public void writeEndArray() throws IOException {
        out.write(XsonType.END_ARRAY.code());
    }

    @Override public void writeStartObject() throws IOException {
        out.write(XsonType.START_OBJECT.code());
    }

    @Override public void writeEndObject() throws IOException {
        out.write(XsonType.END_OBJECT.code());
    }

    @Override public void writeFieldName(String name) throws IOException {
        out.write(XsonType.FIELD_NAME.code());
        outUTF(name);
    }

    @Override public void writeString(String text) throws IOException {
        out.write(XsonType.VALUE_STRING.code());
        outUTF(text);
    }

    @Override public void writeString(char[] text, int offset, int len) throws IOException {
        writeString(new String(text, offset, len));
    }

    @Override public void writeBinary(byte[] data, int offset, int len) throws IOException {
        out.write(XsonType.VALUE_BINARY.code());
        outVInt(len);
        out.write(data, offset, len);
    }

    @Override public void writeBinary(byte[] data) throws IOException {
        out.write(XsonType.VALUE_BINARY.code());
        outVInt(data.length);
        out.write(data);
    }

    @Override public void writeNumber(int v) throws IOException {
        out.write(XsonType.VALUE_INTEGER.code());
        outInt(v);
    }

    @Override public void writeNumber(long v) throws IOException {
        out.write(XsonType.VALUE_LONG.code());
        outLong(v);
    }

    @Override public void writeNumber(double d) throws IOException {
        out.write(XsonType.VALUE_DOUBLE.code());
        outDouble(d);
    }

    @Override public void writeNumber(float f) throws IOException {
        out.write(XsonType.VALUE_FLOAT.code());
        outFloat(f);
    }

    @Override public void writeBoolean(boolean state) throws IOException {
        out.write(XsonType.VALUE_BOOLEAN.code());
        outBoolean(state);
    }

    @Override public void writeNull() throws IOException {
        out.write(XsonType.VALUE_NULL.code());
    }

    @Override public void writeRawFieldStart(String fieldName) throws IOException {
        writeFieldName(fieldName);
    }

    @Override public void flush() throws IOException {
        out.flush();
    }

    @Override public void close() throws IOException {
        out.close();
    }


    private void outShort(short v) throws IOException {
        out.write((byte) (v >> 8));
        out.write((byte) v);
    }

    /**
     * Writes an int as four bytes.
     */
    private void outInt(int i) throws IOException {
        out.write((byte) (i >> 24));
        out.write((byte) (i >> 16));
        out.write((byte) (i >> 8));
        out.write((byte) i);
    }

    /**
     * Writes an int in a variable-length format.  Writes between one and
     * five bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    private void outVInt(int i) throws IOException {
        while ((i & ~0x7F) != 0) {
            out.write((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        out.write((byte) i);
    }

    /**
     * Writes a long as eight bytes.
     */
    private void outLong(long i) throws IOException {
        outInt((int) (i >> 32));
        outInt((int) i);
    }

    /**
     * Writes an long in a variable-length format.  Writes between one and five
     * bytes.  Smaller values take fewer bytes.  Negative numbers are not
     * supported.
     */
    private void outVLong(long i) throws IOException {
        while ((i & ~0x7F) != 0) {
            out.write((byte) ((i & 0x7f) | 0x80));
            i >>>= 7;
        }
        out.write((byte) i);
    }

    /**
     * Writes a string.
     */
    private void outUTF(String s) throws IOException {
        UnicodeUtil.UTF8Result utf8Result = Unicode.unsafeFromStringAsUtf8(s);
        outVInt(utf8Result.length);
        out.write(utf8Result.result, 0, utf8Result.length);
    }

    private void outFloat(float v) throws IOException {
        outInt(Float.floatToIntBits(v));
    }

    private void outDouble(double v) throws IOException {
        outLong(Double.doubleToLongBits(v));
    }


    private static byte ZERO = 0;
    private static byte ONE = 1;

    /**
     * Writes a boolean.
     */
    private void outBoolean(boolean b) throws IOException {
        out.write(b ? ONE : ZERO);
    }
}
