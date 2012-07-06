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

import jsr166y.LinkedTransferQueue;
import org.elasticsearch.common.compress.Compressor;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class CachedStreamOutput {

    private static Entry newEntry() {
        BytesStreamOutput bytes = new BytesStreamOutput();
        HandlesStreamOutput handles = new HandlesStreamOutput(bytes);
        return new Entry(bytes, handles);
    }

    public static class Entry {
        private final BytesStreamOutput bytes;
        private final HandlesStreamOutput handles;

        Entry(BytesStreamOutput bytes, HandlesStreamOutput handles) {
            this.bytes = bytes;
            this.handles = handles;
        }

        public void reset() {
            bytes.reset();
            handles.setOut(bytes);
            handles.clear();
        }

        public BytesStreamOutput bytes() {
            return bytes;
        }

        public StreamOutput handles() throws IOException {
            return handles;
        }

        public StreamOutput bytes(Compressor compressor) throws IOException {
            return compressor.streamOutput(bytes);
        }

        public StreamOutput handles(Compressor compressor) throws IOException {
            StreamOutput compressed = compressor.streamOutput(bytes);
            handles.clear();
            handles.setOut(compressed);
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
    public static int BYTES_LIMIT = 1 * 1024 * 1024; // don't cache entries that are bigger than that...
    public static int COUNT_LIMIT = 100;

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
        entry.reset();
        return entry;
    }

    public static void pushEntry(Entry entry) {
        entry.reset();
        if (entry.bytes().bytes().length() > BYTES_LIMIT) {
            return;
        }
        Queue<Entry> ref = cache.get();
        if (ref == null) {
            ref = new LinkedTransferQueue<Entry>();
            counter.set(0);
            cache.set(ref);
        }
        if (counter.incrementAndGet() > COUNT_LIMIT) {
            counter.decrementAndGet();
        } else {
            ref.add(entry);
        }
    }
}
