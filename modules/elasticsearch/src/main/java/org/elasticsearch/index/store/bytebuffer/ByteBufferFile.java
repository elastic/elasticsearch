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

package org.elasticsearch.index.store.bytebuffer;

import java.nio.ByteBuffer;

/**
 * @author kimchy (Shay Banon)
 */
public class ByteBufferFile {

    private final ByteBufferDirectory dir;

    private volatile long lastModified = System.currentTimeMillis();

    private volatile long length;

    private volatile ByteBuffer[] buffers;

    public ByteBufferFile(ByteBufferDirectory dir) {
        this.dir = dir;
    }

    long lastModified() {
        return lastModified;
    }

    void lastModified(long lastModified) {
        this.lastModified = lastModified;
    }

    long length() {
        return length;
    }

    void length(long length) {
        this.length = length;
    }

    ByteBuffer buffer(int i) {
        return this.buffers[i];
    }

    int numberOfBuffers() {
        return this.buffers.length;
    }

    void buffers(ByteBuffer[] buffers) {
        this.buffers = buffers;
    }

    void clean() {
        if (buffers != null) {
            for (ByteBuffer buffer : buffers) {
                dir.releaseBuffer(buffer);
            }
            buffers = null;
        }
    }
}
