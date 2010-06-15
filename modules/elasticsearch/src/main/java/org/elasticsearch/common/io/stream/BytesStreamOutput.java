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

import org.elasticsearch.common.thread.ThreadLocals;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author kimchy (shay.banon)
 */
public class BytesStreamOutput extends StreamOutput {

    /**
     * A thread local based cache of {@link BytesStreamOutput}.
     */
    public static class Cached {

        private static final ThreadLocal<ThreadLocals.CleanableValue<BytesStreamOutput>> cache = new ThreadLocal<ThreadLocals.CleanableValue<BytesStreamOutput>>() {
            @Override protected ThreadLocals.CleanableValue<BytesStreamOutput> initialValue() {
                return new ThreadLocals.CleanableValue<BytesStreamOutput>(new BytesStreamOutput());
            }
        };

        private static final ThreadLocal<ThreadLocals.CleanableValue<HandlesStreamOutput>> cacheHandles = new ThreadLocal<ThreadLocals.CleanableValue<HandlesStreamOutput>>() {
            @Override protected ThreadLocals.CleanableValue<HandlesStreamOutput> initialValue() {
                return new ThreadLocals.CleanableValue<HandlesStreamOutput>(new HandlesStreamOutput(new BytesStreamOutput()));
            }
        };

        /**
         * Returns the cached thread local byte stream, with its internal stream cleared.
         */
        public static BytesStreamOutput cached() {
            BytesStreamOutput os = cache.get().get();
            os.reset();
            return os;
        }

        public static HandlesStreamOutput cachedHandles() throws IOException {
            HandlesStreamOutput os = cacheHandles.get().get();
            os.reset();
            return os;
        }
    }

    /**
     * The buffer where data is stored.
     */
    protected byte buf[];

    /**
     * The number of valid bytes in the buffer.
     */
    protected int count;

    public BytesStreamOutput() {
        this(126);
    }

    public BytesStreamOutput(int size) {
        this.buf = new byte[size];
    }

    @Override public void writeByte(byte b) throws IOException {
        int newcount = count + 1;
        if (newcount > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        buf[count] = b;
        count = newcount;
    }

    @Override public void writeBytes(byte[] b, int offset, int length) throws IOException {
        if (length == 0) {
            return;
        }
        int newcount = count + length;
        if (newcount > buf.length) {
            buf = Arrays.copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        System.arraycopy(b, offset, buf, count, length);
        count = newcount;
    }

    public void reset() {
        count = 0;
    }

    @Override public void flush() throws IOException {
        // nothing to do there
    }

    @Override public void close() throws IOException {
        // nothing to do here
    }

    /**
     * Creates a newly allocated byte array. Its size is the current
     * size of this output stream and the valid contents of the buffer
     * have been copied into it.
     *
     * @return the current contents of this output stream, as a byte array.
     * @see java.io.ByteArrayOutputStream#size()
     */
    public byte copiedByteArray()[] {
        return Arrays.copyOf(buf, count);
    }

    /**
     * Returns the underlying byte array. Note, use {@link #size()} in order to know
     * the length of it.
     */
    public byte[] unsafeByteArray() {
        return buf;
    }

    /**
     * Returns the current size of the buffer.
     *
     * @return the value of the <code>count</code> field, which is the number
     *         of valid bytes in this output stream.
     * @see java.io.ByteArrayOutputStream#count
     */
    public int size() {
        return count;
    }
}
