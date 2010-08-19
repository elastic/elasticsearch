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
public class CachedStreamInput {

    static class Entry {
        final HandlesStreamInput handles;
        final LZFStreamInput lzf;

        Entry(HandlesStreamInput handles, LZFStreamInput lzf) {
            this.handles = handles;
            this.lzf = lzf;
        }
    }

    private static final ThreadLocal<ThreadLocals.CleanableValue<Entry>> cache = new ThreadLocal<ThreadLocals.CleanableValue<Entry>>() {
        @Override protected ThreadLocals.CleanableValue<Entry> initialValue() {
            HandlesStreamInput handles = new HandlesStreamInput();
            LZFStreamInput lzf = new LZFStreamInput();
            return new ThreadLocals.CleanableValue<Entry>(new Entry(handles, lzf));
        }
    };

    public static LZFStreamInput cachedLzf(StreamInput in) throws IOException {
        LZFStreamInput lzf = cache.get().get().lzf;
        lzf.reset(in);
        return lzf;
    }

    public static HandlesStreamInput cachedHandles(StreamInput in) {
        HandlesStreamInput handles = cache.get().get().handles;
        handles.reset(in);
        return handles;
    }

    public static HandlesStreamInput cachedHandlesLzf(StreamInput in) throws IOException {
        Entry entry = cache.get().get();
        entry.lzf.reset(in);
        entry.handles.reset(entry.lzf);
        return entry.handles;
    }
}
