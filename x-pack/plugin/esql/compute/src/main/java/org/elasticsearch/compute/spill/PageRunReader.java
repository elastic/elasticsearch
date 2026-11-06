/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

/**
 * Decodes the pages of one partition of a page run, in the order they were written. The record's bytes are read into a
 * charged buffer first; decoding then reserves the decoded size, estimated by the record length, before any block is
 * built, and the blocks come from {@code blockFactory}, which charges them as it builds, so the reservation is
 * returned once they exist.
 */
public final class PageRunReader implements Releasable {

    private final FramedRunReader.RecordCursor cursor;
    private final BlockFactory blockFactory;
    private final CircuitBreaker breaker;
    private final TransportVersion version;

    public PageRunReader(FramedRunReader reader, int partition, BlockFactory blockFactory, CircuitBreaker breaker) {
        if (reader.header().kind() != FramedRunLayout.KIND_PAGE) {
            throw new IllegalArgumentException("run is of kind [" + reader.header().kind() + "], not a page run");
        }
        this.cursor = reader.cursor(partition);
        this.blockFactory = blockFactory;
        this.breaker = breaker;
        this.version = reader.transportVersion();
    }

    public boolean hasNext() {
        return cursor.hasNext();
    }

    /**
     * The next page, or null at the end. The page belongs to the caller.
     */
    public void next(ActionListener<Page> listener) {
        cursor.next(listener.delegateFailureAndWrap((l, record) -> {
            if (record == null) {
                l.onResponse(null);
                return;
            }
            Page page;
            try (record) {
                page = decode(record.payload());
            } catch (Exception e) {
                l.onFailure(e);
                return;
            }
            l.onResponse(page);
        }));
    }

    private Page decode(ReleasableBytesReference payload) throws Exception {
        long reserve = payload.length();
        breaker.addEstimateBytesAndMaybeBreak(reserve, "spill page decode");
        try (BlockStreamInput in = new BlockStreamInput(payload.streamInput(), blockFactory)) {
            in.setTransportVersion(version);
            return new Page(in);
        } finally {
            breaker.addWithoutBreaking(-reserve);
        }
    }

    @Override
    public void close() {
        cursor.close();
    }
}
