/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Wraps a {@link CloseableIterator} so the iterator's {@code close()} runs with an
 * {@link ExternalStatsCapture} sink bound on the current thread. The text-format iterators'
 * close hooks call {@code ExternalStatsCapture.record(...)} on the bound sink; whichever
 * operator drove the iteration owns the sink and reads its contents back via
 * {@code AsyncExternalSourceBuffer#capturedSourceMetadataSnapshot()} or equivalent.
 * <p>
 * The wrapper is a thin pass-through for {@code hasNext} / {@code next}; the only behavior
 * change is at close time. Multiple iterators wrapped against the same sink accumulate
 * contributions in the sink — natural fit for multi-file and multi-split paths.
 *
 * <h2>{@link ColumnExtractorProducer} forwarding</h2>
 * The wrapper unconditionally declares the {@link ColumnExtractorProducer} capability and forwards
 * {@link #createColumnExtractor(Consumer)} / {@link #setExtractorId(int)} to its delegate, mirroring
 * {@code SchemaAdaptingIterator}. A non-producer delegate makes the consumer's
 * {@code instanceof ColumnExtractorProducer} check a necessary-but-not-sufficient guard — the
 * dispatch into the delegate fails loud (see {@link #innerProducer()}). Without this forwarding the
 * stats wrapper would hide a producer-capable reader from the deferred-extraction wiring in
 * {@code AsyncExternalSourceOperatorFactory#wrapWithEncoderIfNeeded}, breaking {@code _rowPosition}
 * encoding whenever stats capture and deferred extraction are both active on the same read.
 */
public final class StatsCapturingIterator implements CloseableIterator<Page>, ColumnExtractorProducer {

    private final CloseableIterator<Page> delegate;
    private final ConcurrentMap<String, List<Map<String, Object>>> sink;

    private StatsCapturingIterator(CloseableIterator<Page> delegate, ConcurrentMap<String, List<Map<String, Object>>> sink) {
        this.delegate = delegate;
        this.sink = sink;
    }

    public static CloseableIterator<Page> wrap(CloseableIterator<Page> delegate, ConcurrentMap<String, List<Map<String, Object>>> sink) {
        if (delegate == null || sink == null) {
            return delegate;
        }
        return new StatsCapturingIterator(delegate, sink);
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public Page next() {
        return delegate.next();
    }

    /**
     * Forward the async-ready signal to the delegate. This wrapper is a thin pass-through for
     * {@code hasNext}/{@code next}, so it must be one for readiness too: the producer-loop drain calls
     * {@code waitForReady()} on the outermost iterator and parks the executor thread while it is not
     * done. Without this override the default {@link CloseableIterator#waitForReady()} (immediately
     * done) would swallow the delegate's real signal, and the drain would fall through to a blocking
     * {@code hasNext()} on a parser-backed delegate — the multi-file text-read deadlock.
     */
    @Override
    public SubscribableListener<Void> waitForReady() {
        return delegate.waitForReady();
    }

    @Override
    public Page tryAdvance() {
        return delegate.tryAdvance();
    }

    @Override
    public void close() throws IOException {
        try (ExternalStatsCapture.Handle handle = ExternalStatsCapture.bind(sink)) {
            delegate.close();
        }
    }

    @Override
    public ColumnExtractor createColumnExtractor(@Nullable Consumer<String> driverThreadWarningSink) throws IOException {
        return innerProducer().createColumnExtractor(driverThreadWarningSink);
    }

    @Override
    public void setExtractorId(int id) {
        innerProducer().setExtractorId(id);
    }

    /**
     * Pass-through capability: the producer is the wrapped reader iterator that owns the addressing
     * space and pre-encodes {@code _rowPosition}. If the delegate is not a producer, deferred
     * extraction was wired to a reader that does not support it — fail loudly so callers don't
     * silently lose data.
     */
    private ColumnExtractorProducer innerProducer() {
        if (delegate instanceof ColumnExtractorProducer producer) {
            return producer;
        }
        throw new IllegalStateException(
            "deferred extraction requested but underlying iterator [" + delegate.getClass().getName() + "] is not a ColumnExtractorProducer"
        );
    }
}
