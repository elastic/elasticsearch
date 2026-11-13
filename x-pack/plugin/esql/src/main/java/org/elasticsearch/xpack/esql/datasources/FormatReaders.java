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
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns {@link FormatReader#close()} for copies minted off a longer-lived reader.
 * <p>
 * The registry holds one prototype per format for the node lifetime. {@code with*} copies and
 * {@link CompressionDelegatingFormatReader} wrappers are distinct objects and must be closed at
 * the end of the scope that minted them. Identity {@code with*} returns ({@code this}) are not
 * mints and must not be closed against the prototype.
 * <p>
 * {@link CompressionDelegatingFormatReader#close()} does not close its inner reader: the inner is
 * owned by the prototype or the query-level copy. Walking the wrapper chain here closes each
 * minted wrapper, then the reminted inner, and stops at {@code owner}.
 */
final class FormatReaders {

    private FormatReaders() {}

    /**
     * Close {@code reader} and any compression wrappers it owns, stopping at {@code owner}.
     * No-op when {@code reader} is {@code null} or the same object as {@code owner}.
     */
    static void closeDistinctFrom(@Nullable FormatReader reader, @Nullable FormatReader owner) {
        while (reader != null && reader != owner) {
            FormatReader inner = unwrapCompression(reader);
            IOUtils.closeWhileHandlingException(reader);
            reader = inner;
        }
    }

    /** Close every minted reader that is distinct from {@code owner}. */
    static void closeAll(@Nullable FormatReader owner, FormatReader... minted) {
        if (minted == null) {
            return;
        }
        for (FormatReader reader : minted) {
            closeDistinctFrom(reader, owner);
        }
    }

    /**
     * Close {@code minted} readers when {@code pages} closes. If {@code pages} is {@code null},
     * close them immediately and return {@code null} so a failed open still releases the remint.
     */
    @Nullable
    static CloseableIterator<Page> releasing(
        @Nullable CloseableIterator<Page> pages,
        @Nullable FormatReader owner,
        FormatReader... minted
    ) {
        if (pages == null) {
            closeAll(owner, minted);
            return null;
        }
        return new ReaderReleasingIterator(pages, owner, minted);
    }

    @Nullable
    static FormatReader unwrapCompression(FormatReader reader) {
        return reader instanceof CompressionDelegatingFormatReader wrapped ? wrapped.unwrap() : null;
    }

    private static final class ReaderReleasingIterator implements CloseableIterator<Page> {
        private final CloseableIterator<Page> pages;
        private final FormatReader owner;
        private final FormatReader[] minted;
        private final AtomicBoolean closed = new AtomicBoolean();

        ReaderReleasingIterator(CloseableIterator<Page> pages, FormatReader owner, FormatReader[] minted) {
            this.pages = pages;
            this.owner = owner;
            this.minted = minted;
        }

        @Override
        public boolean hasNext() {
            return pages.hasNext();
        }

        @Override
        public Page next() {
            return pages.next();
        }

        @Override
        public Page tryAdvance() {
            return pages.tryAdvance();
        }

        @Override
        public SubscribableListener<Void> waitForReady() {
            return pages.waitForReady();
        }

        @Override
        public boolean isExhausted() {
            return pages.isExhausted();
        }

        @Override
        public void revokeOvershootOnPark() {
            pages.revokeOvershootOnPark();
        }

        @Override
        public void close() throws IOException {
            if (closed.compareAndSet(false, true) == false) {
                return;
            }
            try {
                pages.close();
            } finally {
                closeAll(owner, minted);
            }
        }
    }
}
