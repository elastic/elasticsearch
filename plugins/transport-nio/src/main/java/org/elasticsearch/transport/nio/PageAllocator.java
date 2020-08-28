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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.nio.Page;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

public class PageAllocator implements IntFunction<Page> {

    private static final int RECYCLE_LOWER_THRESHOLD = PageCacheRecycler.BYTE_PAGE_SIZE / 2;

    private final PageCacheRecycler recycler;

    public PageAllocator(PageCacheRecycler recycler) {
        this.recycler = recycler;
    }

    @Override
    public Page apply(int length) {
        if (length >= RECYCLE_LOWER_THRESHOLD && length <= PageCacheRecycler.BYTE_PAGE_SIZE){
            Recycler.V<byte[]> bytePage = recycler.bytePage(false);
            return new Page(ByteBuffer.wrap(bytePage.v(), 0, length), bytePage::close);
        } else {
            return new Page(ByteBuffer.allocate(length), () -> {});
        }
    }
}
