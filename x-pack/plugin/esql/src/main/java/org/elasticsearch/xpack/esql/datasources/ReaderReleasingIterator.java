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
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractorProducer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Ties a derived {@link FormatReader}'s release to the lifetime of the iterator that reads through it.
 * <p>
 * Needed where a component mints a configured reader ({@code withSchema}, {@code withReadConfig}, …), opens an
 * iterator on it and hands that iterator to a caller: the reader stays in use for as long as the iterator is open,
 * so it cannot be closed when the minting method returns, and the caller cannot close it because it never saw it.
 * The wrapper closes it right after the delegate, which is the order the reader's own state expects — an iterator's
 * close may still call back into the reader (stats finalization, CPU accounting).
 * <p>
 * The reader is closed only when it is not the {@code source} instance the minting component was handed; see the
 * ownership contract on {@link FormatReader}.
 *
 * <h2>{@link ColumnExtractorProducer} forwarding</h2>
 * Like {@code SchemaAdaptingIterator} and {@code StatsCapturingIterator}, this wrapper unconditionally declares the
 * capability and forwards it, so inserting it cannot hide a producer-capable reader iterator from the
 * deferred-extraction wiring in {@code AsyncExternalSourceOperatorFactory#wrapWithEncoderIfNeeded}. A non-producer
 * delegate fails loudly on dispatch rather than silently dropping the {@code _rowPosition} encoding.
 */
final class ReaderReleasingIterator implements CloseableIterator<Page>, ColumnExtractorProducer {

    private final CloseableIterator<Page> delegate;
    private final FormatReader reader;
    private final FormatReader source;

    private ReaderReleasingIterator(CloseableIterator<Page> delegate, FormatReader reader, @Nullable FormatReader source) {
        this.delegate = delegate;
        this.reader = reader;
        this.source = source;
    }

    /**
     * Wraps {@code delegate} so closing it also releases {@code reader}. Returns {@code delegate} unchanged when
     * there is nothing to release — {@code reader} is the very instance {@code source} the caller was handed, so no
     * {@code with*} call minted anything and no ownership changed hands.
     */
    static CloseableIterator<Page> wrap(CloseableIterator<Page> delegate, FormatReader reader, @Nullable FormatReader source) {
        if (delegate == null || reader == null || reader == source) {
            return delegate;
        }
        return new ReaderReleasingIterator(delegate, reader, source);
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
            FormatReaderOwnership.closeIfDerived(reader, source);
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
