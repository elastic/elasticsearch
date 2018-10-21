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

import java.io.EOFException;
import java.io.IOException;

/**
 * Wraps a {@link StreamInput} and delegates to it. To be used to add functionality to an existing stream by subclassing.
 */
public abstract class FilterStreamInput extends StreamInput {

    protected final StreamInput delegate;

    protected FilterStreamInput(StreamInput delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte readByte() throws IOException {
        return delegate.readByte();
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        delegate.readBytes(b, offset, len);
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public int read() throws IOException {
        return delegate.read();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public Version getVersion() {
        return delegate.getVersion();
    }

    @Override
    public void setVersion(Version version) {
        delegate.setVersion(version);
    }

    @Override
    protected void ensureCanReadBytes(int length) throws EOFException {
        delegate.ensureCanReadBytes(length);
    }
}
