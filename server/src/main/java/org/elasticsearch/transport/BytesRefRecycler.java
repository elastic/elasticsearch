/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;

public class BytesRefRecycler implements Recycler<BytesRef> {

    public static final BytesRefRecycler NON_RECYCLING_INSTANCE = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);

    private final PageCacheRecycler recycler;

    public BytesRefRecycler(PageCacheRecycler recycler) {
        this.recycler = recycler;
    }

    @Override
    public Recycler.V<BytesRef> obtain() {
        Recycler.V<byte[]> v = recycler.bytePage(false);
        BytesRef bytesRef = new BytesRef(v.v(), 0, PageCacheRecycler.BYTE_PAGE_SIZE);
        return new Recycler.V<>() {
            @Override
            public BytesRef v() {
                return bytesRef;
            }

            @Override
            public boolean isRecycled() {
                return v.isRecycled();
            }

            @Override
            public void close() {
                v.close();
            }
        };
    }
}
