/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;

/**
 * Where a codec task sends the chunks it serializes, one at a time, in order. Provisional: the storage SPI's writer takes
 * this shape over.
 * <p>
 * The borrow rule: the chunk belongs to the caller. A sink that needs it past the call takes its own reference with
 * {@link ReleasableBytesReference#retain()} and releases that reference before it completes the listener; after the
 * terminal callback the caller releases its own, and the buffer behind the chunk is reused. The caller never appends a
 * second chunk before the listener of the first has completed. The listener may complete inline.
 */
public interface ChunkSink {

    void append(ReleasableBytesReference chunk, ActionListener<Void> listener);
}
