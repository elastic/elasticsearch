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

import org.elasticsearch.common.util.concurrent.jsr166y.LinkedTransferQueue;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kimchy (shay.banon)
 */
public class CachedStreamOutput {

    private static Entry newEntry() {
        BytesStreamOutput bytes = new BytesStreamOutput();
        HandlesStreamOutput handles = new HandlesStreamOutput(bytes);
        LZFStreamOutput lzf = new LZFStreamOutput(bytes, true);
        return new Entry(bytes, handles, lzf);
    }

    public static class Entry {
        private final BytesStreamOutput bytes;
        private final HandlesStreamOutput handles;
        private final LZFStreamOutput lzf;

        Entry(BytesStreamOutput bytes, HandlesStreamOutput handles, LZFStreamOutput lzf) {
            this.bytes = bytes;
            this.handles = handles;
            this.lzf = lzf;
        }

        /**
         * Returns the underlying bytes without any resetting.
         */
        public BytesStreamOutput bytes() {
            return bytes;
        }

        /**
         * Returns cached bytes that are also reset.
         */
        public BytesStreamOutput cachedBytes() {
            bytes.reset();
            return bytes;
        }

        public LZFStreamOutput cachedLZFBytes() throws IOException {
            lzf.reset();
            return lzf;
        }

        public HandlesStreamOutput cachedHandlesLzfBytes() throws IOException {
            handles.reset(lzf);
            return handles;
        }

        public HandlesStreamOutput cachedHandlesBytes() throws IOException {
            handles.reset(bytes);
            return handles;
        }
    }

    static class SoftWrapper<T> {
        private SoftReference<T> ref;

        public SoftWrapper() {
        }

        public void set(T ref) {
            this.ref = new SoftReference<T>(ref);
        }

        public T get() {
            return ref == null ? null : ref.get();
        }

        public void clear() {
            ref = null;
        }
    }

    private static final SoftWrapper<Queue<Entry>> cache = new SoftWrapper<Queue<Entry>>();
    private static final AtomicInteger counter = new AtomicInteger();
    private static final int BYTES_LIMIT = 10 * 1024 * 1024; // don't cache entries that are bigger than that...
    private static final int COUNT_LIMIT = 100;

    public static void clear() {
        cache.clear();
    }

    public static Entry popEntry() {
        Queue<Entry> ref = cache.get();
        if (ref == null) {
            return newEntry();
        }
        Entry entry = ref.poll();
        if (entry == null) {
            return newEntry();
        }
        counter.decrementAndGet();
        return entry;
    }

    public static void pushEntry(Entry entry) {
        if (entry.bytes().underlyingBytes().length > BYTES_LIMIT) {
            return;
        }
        Queue<Entry> ref = cache.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<Entry>();
            cache.set(ref);
        }
        if (counter.incrementAndGet() > COUNT_LIMIT) {
            counter.decrementAndGet();
        } else {
            ref.add(entry);
        }
    }
}
