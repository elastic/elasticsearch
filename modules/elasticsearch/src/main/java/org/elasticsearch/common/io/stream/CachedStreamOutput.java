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

/**
 * @author kimchy (shay.banon)
 */
public class CachedStreamOutput {

    static class Entry {
        final BytesStreamOutput bytes;
        final HandlesStreamOutput handles;
        final LZFStreamOutput lzf;

        Entry(BytesStreamOutput bytes, HandlesStreamOutput handles, LZFStreamOutput lzf) {
            this.bytes = bytes;
            this.handles = handles;
            this.lzf = lzf;
        }
    }

    private static final ThreadLocal<ThreadLocals.CleanableValue<Entry>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Entry>>() {
        @Override protected ThreadLocals.CleanableValue<Entry> initialValue() {
            BytesStreamOutput bytes = new BytesStreamOutput();
            HandlesStreamOutput handles = new HandlesStreamOutput(bytes);
            LZFStreamOutput lzf = new LZFStreamOutput(bytes);
            return new ThreadLocals.CleanableValue<Entry>(new Entry(bytes, handles, lzf));
        }
    };

    /**
     * Returns the cached thread local byte stream, with its internal stream cleared.
     */
    public static BytesStreamOutput cachedBytes() {
        BytesStreamOutput os = cache.get().get().bytes;
        os.reset();
        return os;
    }

    public static LZFStreamOutput cachedLZFBytes() throws IOException {
        LZFStreamOutput lzf = cache.get().get().lzf;
        lzf.reset();
        return lzf;
    }

    public static HandlesStreamOutput cachedHandlesLzfBytes() throws IOException {
        Entry entry = cache.get().get();
        HandlesStreamOutput os = entry.handles;
        os.reset(entry.lzf);
        return os;
    }

    public static HandlesStreamOutput cachedHandlesBytes() throws IOException {
        Entry entry = cache.get().get();
        HandlesStreamOutput os = entry.handles;
        os.reset(entry.bytes);
        return os;
    }
}
