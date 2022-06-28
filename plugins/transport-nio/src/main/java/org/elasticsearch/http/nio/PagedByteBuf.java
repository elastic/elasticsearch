/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.buffer.UnpooledHeapByteBuf;

import org.elasticsearch.nio.Page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PagedByteBuf extends UnpooledHeapByteBuf {

    private final Runnable releasable;

    private PagedByteBuf(byte[] array, Runnable releasable) {
        super(UnpooledByteBufAllocator.DEFAULT, array, array.length);
        this.releasable = releasable;
    }

    static ByteBuf byteBufFromPages(Page[] pages) {
        int componentCount = pages.length;
        if (componentCount == 0) {
            return Unpooled.EMPTY_BUFFER;
        } else if (componentCount == 1) {
            return byteBufFromPage(pages[0]);
        } else {
            int maxComponents = Math.max(16, componentCount);
            final List<ByteBuf> components = new ArrayList<>(componentCount);
            for (Page page : pages) {
                components.add(byteBufFromPage(page));
            }
            return new CompositeByteBuf(UnpooledByteBufAllocator.DEFAULT, false, maxComponents, components);
        }
    }

    private static ByteBuf byteBufFromPage(Page page) {
        ByteBuffer buffer = page.byteBuffer();
        assert buffer.isDirect() == false && buffer.hasArray() : "Must be a heap buffer with an array";
        int offset = buffer.arrayOffset() + buffer.position();
        PagedByteBuf newByteBuf = new PagedByteBuf(buffer.array(), page::close);
        return newByteBuf.slice(offset, buffer.remaining());
    }

    @Override
    protected void deallocate() {
        try {
            super.deallocate();
        } finally {
            releasable.run();
        }
    }
}
