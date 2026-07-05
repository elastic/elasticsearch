/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;

import java.util.Arrays;

/**
 * A {@link Recycler} for {@link BytesRef} pages which clears the obtained pages on release, out of an abundance of caution when the pages
 * may contain sensitive data.
 */
public final class SecureBytesRefRecycler implements Recycler<BytesRef> {

    private final Recycler<BytesRef> delegate;

    public SecureBytesRefRecycler(Recycler<BytesRef> delegate) {
        this.delegate = delegate;
    }

    @Override
    public V<BytesRef> obtain() {
        return new V<>() {
            final V<BytesRef> v = delegate.obtain();

            @Override
            public BytesRef v() {
                return v.v();
            }

            @Override
            public boolean isRecycled() {
                return v.isRecycled();
            }

            @Override
            public void close() {
                clear(v);
                v.close();
            }
        };
    }

    private static void clear(V<BytesRef> v) {
        final var page = v.v();
        Arrays.fill(page.bytes, page.offset, page.offset + page.length, (byte) 0);
    }

    @Override
    public int pageSize() {
        return delegate.pageSize();
    }
}
