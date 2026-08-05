/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.exchange.ExchangeSink;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Copies the pages one batch's data drivers emit, so that a batch that turns out to be storable can be stored without
 * re-running it. Pages are serialized as they pass through the exchange sink, because that is the last point at which
 * the batch still owns them.
 * <p>
 * A shard's output is routinely more than one page - an aggregation emits an intermediate page whenever its partial
 * emit thresholds are crossed - and exchange transport carries at most one page per response, so there is no existing
 * container to borrow. The stored value is therefore a page count followed by the pages, so a hit replays exactly the
 * sequence that was stored rather than truncating to the last page or mis-parsing a bare concatenation.
 * <p>
 * Several drivers share one capture (a shard's slices are stolen across {@code taskConcurrency} drivers), so
 * {@link #record} and {@link #value} are synchronized. Page order is not preserved and does not need to be: the
 * exchange buffer they feed is already an unordered concurrent queue, so no consumer may depend on the order.
 */
final class ShardResultCapture {

    private static final Logger logger = LogManager.getLogger(ShardResultCapture.class);

    private final long maxValueSizeInBytes;
    private final List<BytesReference> pages = new ArrayList<>();
    private long totalBytes;
    private boolean usable = true;

    ShardResultCapture(long maxValueSizeInBytes) {
        this.maxValueSizeInBytes = maxValueSizeInBytes;
    }

    /**
     * The captured value, or {@code null} when this batch produced nothing storable: the size cap was exceeded, or a
     * page could not be serialized. The latter should not happen for an admitted plan shape - a doc column, for one,
     * is not serializable at all - but it is caught rather than asserted, because failing to cache is a far better
     * outcome than failing the query.
     */
    @Nullable
    synchronized BytesReference value() {
        if (usable == false) {
            return null;
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            out.writeVInt(pages.size());
            BytesReference[] parts = new BytesReference[pages.size() + 1];
            parts[0] = out.bytes();
            for (int i = 0; i < pages.size(); i++) {
                parts[i + 1] = pages.get(i);
            }
            return CompositeBytesReference.of(parts);
        } catch (Exception e) {
            logger.debug("failed to assemble a shard result cache value", e);
            return null;
        }
    }

    private void record(Page page) {
        BytesReference bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.current());
            page.writeTo(out);
            bytes = out.bytes();
        } catch (Exception e) {
            logger.debug("failed to capture a page for the shard result cache", e);
            synchronized (this) {
                discard();
            }
            return;
        }
        synchronized (this) {
            if (usable == false) {
                return;
            }
            totalBytes += bytes.length();
            if (totalBytes > maxValueSizeInBytes) {
                discard();
                return;
            }
            pages.add(bytes);
        }
    }

    /** Must be called while holding the monitor on {@code this}. */
    private void discard() {
        usable = false;
        pages.clear();
    }

    /**
     * Wraps a sink so that everything written to it is captured on the way through. Serialization happens before the
     * page is handed on, while this thread is still the only owner.
     */
    ExchangeSink wrap(ExchangeSink delegate) {
        return new ExchangeSink() {
            @Override
            public void addPage(Page page) {
                record(page);
                delegate.addPage(page);
            }

            @Override
            public void finish() {
                delegate.finish();
            }

            @Override
            public boolean isFinished() {
                return delegate.isFinished();
            }

            @Override
            public void addCompletionListener(ActionListener<Void> listener) {
                delegate.addCompletionListener(listener);
            }

            @Override
            public IsBlockedResult waitForWriting() {
                return delegate.waitForWriting();
            }
        };
    }
}
