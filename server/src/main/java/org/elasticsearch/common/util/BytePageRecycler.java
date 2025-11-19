/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.function.IntFunction;

public class BytePageRecycler implements Recycler<BytesRef> {

    private final VarPageRecycler varPageRecycler;
    private final PageCacheRecycler pageCacheRecycler;

    public BytePageRecycler(@Nullable VarPageRecycler varPageRecycler, PageCacheRecycler pageCacheRecycler) {
        this.varPageRecycler = varPageRecycler;
        this.pageCacheRecycler = pageCacheRecycler;
    }

    public Recycler.V<BytesRef> getPage(int bytesRequested) {
        if (varPageRecycler != null) {
            return varPageRecycler.apply(bytesRequested);
        } else {
            return obtain();
        }
    }

    @Override
    public V<BytesRef> obtain() {
        if (varPageRecycler != null) {
            return varPageRecycler.obtain();
        } else {
            V<byte[]> v = pageCacheRecycler.bytePage(false);
            return new Page(v.v(), 0, PageCacheRecycler.BYTE_PAGE_SIZE, v.isRecycled(), v);
        }
    }

    @Override
    public int pageSize() {
        return PageCacheRecycler.BYTE_PAGE_SIZE;
    }

    public interface VarPageRecycler extends Recycler<BytesRef>, IntFunction<Page> {}

    public static class Page implements V<BytesRef> {

        private final Releasable releasable;
        private final BytesRef bytesRef;
        private final boolean isRecycled;

        private Page(byte[] bytes, int offset, int length, boolean isRecycled, Releasable releasable) {
            this.isRecycled = isRecycled;
            this.bytesRef = new BytesRef(bytes, offset, length);
            this.releasable = releasable;
        }

        @Override
        public BytesRef v() {
            return bytesRef;
        }

        @Override
        public boolean isRecycled() {
            return isRecycled;
        }

        @Override
        public void close() {
            releasable.close();
        }
    }
}
