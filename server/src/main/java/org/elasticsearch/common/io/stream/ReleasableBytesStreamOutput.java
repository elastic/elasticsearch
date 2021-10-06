/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;

/**
 * An bytes stream output that allows providing a {@link BigArrays} instance
 * expecting it to require releasing its content ({@link #bytes()}) once done.
 * <p>
 * Please note, closing this stream will release the bytes that are in use by any
 * {@link ReleasableBytesReference} returned from {@link #bytes()}, so this
 * stream should only be closed after the bytes have been output or copied
 * elsewhere.
 */
public class ReleasableBytesStreamOutput extends BytesStreamOutput implements Releasable {

    public ReleasableBytesStreamOutput(BigArrays bigarrays) {
        this(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigarrays);
    }

    public ReleasableBytesStreamOutput(int expectedSize, BigArrays bigArrays) {
        super(expectedSize, bigArrays);
    }

    @Override
    public void close() {
        Releasables.close(bytes);
    }

    @Override
    public void reset() {
        assert false;
        // not supported, close and create a new instance instead
        throw new UnsupportedOperationException("must not reuse a pooled bytes backed stream");
    }
}
