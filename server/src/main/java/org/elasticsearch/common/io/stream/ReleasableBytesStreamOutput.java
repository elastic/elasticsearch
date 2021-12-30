/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * A @link {@link StreamOutput} that uses {@link Recycler.V<BytesRef>} to acquire pages of bytes, which
 * avoids frequent reallocation &amp; copying of the internal data. When {@link #close()} is called,
 * the bytes will be released.
 */
public class ReleasableBytesStreamOutput extends BytesStreamOutput implements Releasable {

    public ReleasableBytesStreamOutput(Recycler<BytesRef> recycler) {
        super(PageCacheRecycler.BYTE_PAGE_SIZE, recycler);
    }

    public ReleasableBytesStreamOutput(int expectedSize, Recycler<BytesRef> recycler) {
        super(expectedSize, recycler);
    }

    @Override
    public void reset() {
        Releasables.close(pages);
        super.reset();
    }

    @Override
    public void close() {
        try {
            Releasables.close(pages);
        } finally {
            pages.clear();
        }
    }
}
