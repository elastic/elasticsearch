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

package org.elasticsearch.index.store;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

/** 
 * abstract class for verifying what was written.
 * subclasses override {@link #writeByte(byte)} and {@link #writeBytes(byte[], int, int)}
 */
// do NOT optimize this class for performance
public abstract class VerifyingIndexOutput extends IndexOutput {
    protected final IndexOutput out;
    private final String name;

    /** Sole constructor */
    VerifyingIndexOutput(IndexOutput out, String name) {
        this.out = out;
        this.name = name;
    }
    
    /**
     * Verifies the checksum and compares the written length with the expected file length. This method should be
     * called after all data has been written to this output.
     */
    public abstract void verify() throws IOException;
    
    // default implementations... forwarding to delegate
    
    @Override
    public final void close() throws IOException {
        out.close();
    }

    @Override
    @Deprecated
    public final void flush() throws IOException {
        out.flush(); // we dont buffer, but whatever
    }

    @Override
    public final long getChecksum() throws IOException {
        return out.getChecksum();
    }

    @Override
    public final long getFilePointer() {
        return out.getFilePointer();
    }
    
    @Override
    public final long length() throws IOException {
        return out.length();
    }

    /**
     * Returns the name of the resource to verfiy
     */
    public final String getName() {
        return name;
    }

    public final String toString() {
        return "(resource=" + out + ")(name=" + name + ")"; // out.toString is buggy in 4.10.x so we also append the name here to see which file we verify
    }
}
