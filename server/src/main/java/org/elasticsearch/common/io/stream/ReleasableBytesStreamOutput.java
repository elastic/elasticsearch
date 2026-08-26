/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

/**
 * A @link {@link StreamOutput} that accumulates the resulting data in memory, using {@link BigArrays} to avoids frequent reallocation &amp;
 * copying of the internal data once the resulting data grows large enough whilst avoiding excessive overhead in the final result for small
 * objects.
 * <p>
 * A {@link ReleasableBytesStreamOutput} behaves similarly to a {@link BytesStreamOutput} except that it accumulates data using the provided
 * {@link BigArrays}, which typically should be a recycling instance and thus the resulting bytes must be explicitly released when no
 * longer needed. As with the {@link BytesStreamOutput} it uses a thread-locally-cached buffer for some of its
 * writes and pushes data to the underlying array in small chunks, causing frequent calls to {@link BigArrays#resize}. If the array is large
 * enough (≥8kiB) then the resize operations happen in-place, obtaining a recycled 16kiB page and appending it to the array, but for smaller
 * arrays these resize operations allocate a completely fresh {@code byte[]} into which they copy the entire contents of the old one.
 * <p>
 * As above, smaller arrays grow slowly into freshly-allocated {@code byte[]} arrays with a target of 12.5% overhead. It may be worth adding
 * a {@link BufferedStreamOutput} wrapper to reduce the frequency of the resize operations, especially if a suitable buffer is already
 * allocated and available.
 * <p>
 * This is different from a {@link RecyclerBytesStreamOutput} which <i>only</i> uses recycled 16kiB pages and never itself allocates a raw
 * {@code byte[]}. However, note that by default a {@link ReleasableBytesStreamOutput} uses {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES}
 * for its {@code expectedSize} so that it also always starts by using a recycled page rather than a slow-growing fresh {@code byte[]}.
 * <p>
 * The resulting {@link ReleasableBytesReference} is a view over the underlying {@code byte[]} pages and involves no significant extra
 * allocation to obtain. It is oversized: The worst case for overhead is when the data is one byte more than a 16kiB page and therefore the
 * result must retain two pages even though all but one byte of the second page is unused. The recycling {@link BigArrays} also switches to
 * using recycled pages at half a page (8kiB) which also carries around 50% overhead. For smaller objects the overhead will be 12.5%.
 * <p>
 * Any memory allocated in this way is tracked by the {@link org.elasticsearch.common.breaker} subsystem if using a suitably-configured
 * {@link BigArrays}.
 * <p>
 * Please note, closing this stream will release the bytes that are in use by any {@link ReleasableBytesReference} returned from
 * {@link #bytes()}, so this stream should only be closed after the bytes have been output or copied elsewhere.
 */
public class ReleasableBytesStreamOutput extends BytesStreamOutput implements Releasable {

    /**
     * Create a {@link ReleasableBytesStreamOutput}, acquiring from the given {@link BigArrays} a single recycled page for the initial
     * buffer, and growing the buffer as needed.
     */
    public ReleasableBytesStreamOutput(BigArrays bigarrays) {
        this(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigarrays);
    }

    /**
     * Create a {@link ReleasableBytesStreamOutput}, allocating an initial buffer of size {@code expectedSize} from the given
     * {@link BigArrays}, and growing the buffer as needed.
     * <p>
     * Note that if {@code expectedSize < PageCacheRecycler.PAGE_SIZE_IN_BYTES / 2} then this will allocate a {@code new byte[]} rather than
     * using a recycled page, and will keep on allocating {@code new byte[]} instances, copying the contents, until the contents reach
     * {@code PageCacheRecycler.PAGE_SIZE_IN_BYTES / 2}. In the worst case this can be over 40 allocations before it gets big enough to
     * start using recycled pages. This is probably not what you want.
     */
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
