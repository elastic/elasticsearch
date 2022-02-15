/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        if (length >= RECYCLE_LOWER_THRESHOLD && length <= PageCacheRecycler.BYTE_PAGE_SIZE) {
            Recycler.V<byte[]> bytePage = recycler.bytePage(false);
            return new Page(ByteBuffer.wrap(bytePage.v(), 0, length), bytePage);
        } else {
            return new Page(ByteBuffer.allocate(length), () -> {});
        }
    }
}
