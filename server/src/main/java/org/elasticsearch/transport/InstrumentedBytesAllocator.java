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

package org.elasticsearch.transport;

import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageAllocator;
import org.elasticsearch.common.util.PageCacheRecycler;

import java.util.concurrent.atomic.LongAdder;

public class InstrumentedBytesAllocator implements PageAllocator {

    private final PageCacheRecycler delegate;
    private final LongAdder bytesActive = new LongAdder();

    public InstrumentedBytesAllocator(PageCacheRecycler delegate) {
        this.delegate = delegate;
    }

    @Override
    public Recycler.V<byte[]> bytePage(boolean clear) {
        Recycler.V<byte[]> page = delegate.bytePage(clear);
        int bytesAllocated = page.v().length;
        bytesActive.add(bytesAllocated);
        return new Recycler.V<>() {
            @Override
            public byte[] v() {
                return page.v();
            }

            @Override
            public boolean isRecycled() {
                return page.isRecycled();
            }

            @Override
            public void close() {
                bytesActive.add(-bytesAllocated);
                page.close();
            }
        };
    }

    public long bytesPoolSize() {
        return PageCacheRecycler.BYTE_PAGE_SIZE * delegate.bytePageCount();
    }

    public long allocationAmount() {
        return bytesActive.longValue();
    }

    @Override
    public Recycler.V<int[]> intPage(boolean clear) {
        return delegate.intPage(clear);
    }

    @Override
    public Recycler.V<long[]> longPage(boolean clear) {
        return delegate.longPage(clear);
    }

    @Override
    public Recycler.V<Object[]> objectPage() {
        return delegate.objectPage();
    }
}
