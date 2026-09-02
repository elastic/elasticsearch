/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Ties an owned {@link FormatReader}'s release to the lifetime of the iterator that reads through it.
 * <p>
 * Needed where a component builds a reader, opens an iterator on it, and hands that iterator to a caller:
 * the reader stays in use for as long as the iterator is open,
 * so it cannot be closed when the minting method returns, and the caller cannot close it because it never saw it.
 * The wrapper closes it right after the delegate, which is the order the reader's own state expects. An iterator's
 * close may still call back into the reader (stats finalization, CPU accounting).
 *
 * <h2>{@link ColumnExtractorProducer} forwarding</h2>
 * Like {@code SchemaAdaptingIterator} and {@code StatsCapturingIterator}, this wrapper unconditionally declares the
 * capability and forwards it, so inserting it cannot hide a producer-capable reader iterator from the
 * deferred-extraction wiring in {@code AsyncExternalSourceOperatorFactory#wrapWithEncoderIfNeeded}. A non-producer
 * delegate fails loudly on dispatch rather than silently dropping the {@code _rowPosition} encoding.
 */
final class ReaderReleasingIterator implements CloseableIterator<Page>, ColumnExtractorProducer {

    private static final Logger logger = LogManager.getLogger(ReaderReleasingIterator.class);

    private final CloseableIterator<Page> delegate;
    private final FormatReader reader;

    private ReaderReleasingIterator(CloseableIterator<Page> delegate, FormatReader reader) {
        this.delegate = Objects.requireNonNull(delegate);
        this.reader = Objects.requireNonNull(reader);
    }

    /** Wraps {@code delegate} so closing it also releases {@code reader}. */
    static CloseableIterator<Page> wrap(CloseableIterator<Page> delegate, FormatReader reader) {
        return new ReaderReleasingIterator(delegate, reader);
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
     * Forward the async-ready signal: the producer-loop drain calls {@code waitForReady()} on the outermost
     * iterator and parks while it is not done. Swallowing a parser-backed delegate's real signal behind the
     * interface default (immediately done) would drop the drain into a blocking {@code hasNext()}.
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
        try {
            delegate.close();
        } finally {
            closeReader(reader);
        }
    }

    /**
     * Closes {@code reader} and swallows the failure so cleanup cannot replace a successful result.
     */
    static void closeReader(@Nullable FormatReader reader) {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        } catch (Exception e) {
            logger.warn("failed to close format reader [{}]", reader.getClass().getName(), e);
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

    private ColumnExtractorProducer innerProducer() {
        if (delegate instanceof ColumnExtractorProducer producer) {
            return producer;
        }
        throw new IllegalStateException(
            "deferred extraction requested but underlying iterator [" + delegate.getClass().getName() + "] is not a ColumnExtractorProducer"
        );
    }
}
