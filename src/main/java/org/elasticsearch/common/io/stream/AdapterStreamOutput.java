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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;

import java.io.IOException;

/**
 */
public class AdapterStreamOutput extends StreamOutput {

    protected StreamOutput out;

    public AdapterStreamOutput(StreamOutput out) {
        this.out = out;
    }

    public void setOut(StreamOutput out) {
        this.out = out;
    }

    public StreamOutput wrappedOut() {
        return this.out;
    }

    @Override
    public boolean seekPositionSupported() {
        return out.seekPositionSupported();
    }

    @Override
    public long position() throws IOException {
        return out.position();
    }

    @Override
    public void seek(long position) throws IOException {
        out.seek(position);
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.writeBytes(b, offset, length);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void reset() throws IOException {
        out.reset();
    }

    @Override
    public void writeBytes(byte[] b) throws IOException {
        out.writeBytes(b);
    }

    @Override
    public void writeBytes(byte[] b, int length) throws IOException {
        out.writeBytes(b, length);
    }

    @Override
    public void writeBytesReference(@Nullable BytesReference bytes) throws IOException {
        out.writeBytesReference(bytes);
    }

    @Override
    public void writeInt(int i) throws IOException {
        out.writeInt(i);
    }

    @Override
    public void writeVInt(int i) throws IOException {
        out.writeVInt(i);
    }

    @Override
    public void writeLong(long i) throws IOException {
        out.writeLong(i);
    }

    @Override
    public void writeVLong(long i) throws IOException {
        out.writeVLong(i);
    }

    @Override
    public void writeUTF(String str) throws IOException {
        out.writeUTF(str);
    }

    @Override
    public void writeString(String str) throws IOException {
        out.writeString(str);
    }

    @Override
    public void writeText(Text text) throws IOException {
        out.writeText(text);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        out.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        out.writeDouble(v);
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        out.writeBoolean(b);
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    @Override
    public String toString() {
        return out.toString();
    }
}
