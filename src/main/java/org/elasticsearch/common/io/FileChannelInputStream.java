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

package org.elasticsearch.common.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 */
public class FileChannelInputStream extends InputStream {

    private final FileChannel channel;

    private long position;

    private long length;

    private ByteBuffer bb = null;
    private byte[] bs = null; // Invoker's previous array
    private byte[] b1 = null;

    private long markPosition;

    /**
     * @param channel  The channel to read from
     * @param position The position to start reading from
     * @param length   The length to read
     */
    public FileChannelInputStream(FileChannel channel, long position, long length) {
        this.channel = channel;
        this.position = position;
        this.markPosition = position;
        this.length = position + length; // easier to work with total length
    }

    @Override
    public int read() throws IOException {
        if (b1 == null) {
            b1 = new byte[1];
        }
        int n = read(b1);
        if (n == 1) {
            return b1[0] & 0xff;
        }
        return -1;
    }

    @Override
    public int read(byte[] bs, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        if ((length - position) < len) {
            len = (int) (length - position);
        }

        if (len == 0) {
            return -1;
        }

        ByteBuffer bb = ((this.bs == bs) ? this.bb : ByteBuffer.wrap(bs));
        bb.limit(Math.min(off + len, bb.capacity()));
        bb.position(off);

        this.bb = bb;
        this.bs = bs;
        int read = channel.read(bb, position);
        if (read > 0) {
            position += read;
        }
        return read;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readlimit) {
        this.markPosition = position;
    }

    @Override
    public void reset() throws IOException {
        position = markPosition;
    }
}
