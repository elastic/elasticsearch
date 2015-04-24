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
package org.elasticsearch.common.lucene.store;

import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * IndexOutput that delegates all calls to another IndexOutput
 */
public class FilterIndexOutput extends IndexOutput {

    protected final IndexOutput out;

    public FilterIndexOutput(String resourceDescription, IndexOutput out) {
        super(resourceDescription);
        this.out = out;
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public long getFilePointer() {
        return out.getFilePointer();
    }

    @Override
    public long getChecksum() throws IOException {
        return out.getChecksum();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.writeBytes(b, offset, length);
    }
}
