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

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.text.Text;

import java.io.IOException;

/**
 */
public abstract class AdapterStreamInput extends StreamInput {

    protected StreamInput in;

    protected AdapterStreamInput() {
    }

    public AdapterStreamInput(StreamInput in) {
        this.in = in;
        super.setVersion(in.getVersion());
    }

    @Override
    public StreamInput setVersion(Version version) {
        in.setVersion(version);
        return super.setVersion(version);
    }

    public void reset(StreamInput in) {
        this.in = in;
    }

    @Override
    public byte readByte() throws IOException {
        return in.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        in.readBytes(b, offset, len);
    }

    @Override
    public BytesReference readBytesReference() throws IOException {
        return in.readBytesReference();
    }

    @Override
    public BytesReference readBytesReference(int length) throws IOException {
        return in.readBytesReference(length);
    }

    @Override
    public void reset() throws IOException {
        in.reset();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    // override ones to direct them


    @Override
    public void readFully(byte[] b) throws IOException {
        in.readFully(b);
    }

    @Override
    public short readShort() throws IOException {
        return in.readShort();
    }

    @Override
    public int readInt() throws IOException {
        return in.readInt();
    }

    @Override
    public int readVInt() throws IOException {
        return in.readVInt();
    }

    @Override
    public long readLong() throws IOException {
        return in.readLong();
    }

    @Override
    public long readVLong() throws IOException {
        return in.readVLong();
    }

    @Override
    public String readString() throws IOException {
        return in.readString();
    }

    @Override
    public String readSharedString() throws IOException {
        return in.readSharedString();
    }

    @Override
    public Text readText() throws IOException {
        return in.readText();
    }

    @Override
    public Text readSharedText() throws IOException {
        return in.readSharedText();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return in.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return in.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return in.skip(n);
    }

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public void mark(int readlimit) {
        in.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public String toString() {
        return in.toString();
    }
}
