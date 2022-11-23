/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CountingPageCacheRecycler extends PageCacheRecycler {

    private int openPages = 0;

    public CountingPageCacheRecycler() {
        super(Settings.EMPTY);
    }

    @Override
    public Recycler.V<byte[]> bytePage(boolean clear) {
        final var page = super.bytePage(clear);
        openPages += 1;
        return new Recycler.V<>() {
            boolean closed = false;

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
                assertFalse(closed);
                closed = true;
                openPages -= 1;
                page.close();
            }
        };
    }

    @Override
    public Recycler.V<Object[]> objectPage() {
        throw new AssertionError("unexpected call to objectPage()");
    }

    public void assertAllPagesReleased() {
        assertEquals(0, openPages);
    }
}
