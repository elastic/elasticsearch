/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.Page;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

public class SSLOutboundBuffer implements AutoCloseable {

    private final ArrayDeque<Page> pages;
    private final IntFunction<Page> pageSupplier;

    private Page currentPage;

    SSLOutboundBuffer(IntFunction<Page> pageSupplier) {
        this.pages = new ArrayDeque<>();
        this.pageSupplier = pageSupplier;
    }

    void incrementEncryptedBytes(int encryptedBytesProduced) {
        if (encryptedBytesProduced != 0) {
            currentPage.byteBuffer().limit(encryptedBytesProduced);
            pages.addLast(currentPage);
        } else if (currentPage != null) {
            currentPage.close();
        }
        currentPage = null;
    }

    ByteBuffer nextWriteBuffer(int networkBufferSize) {
        if (currentPage != null) {
            // If there is an existing page, close it as it wasn't large enough to accommodate the SSLEngine.
            currentPage.close();
        }

        Page newPage = pageSupplier.apply(networkBufferSize);
        currentPage = newPage;
        return newPage.byteBuffer().duplicate();
    }

    FlushOperation buildNetworkFlushOperation() {
        return buildNetworkFlushOperation((r, e) -> {});
    }

    FlushOperation buildNetworkFlushOperation(BiConsumer<Void, Exception> listener) {
        int pageCount = pages.size();
        ByteBuffer[] byteBuffers = new ByteBuffer[pageCount];
        Page[] pagesToClose = new Page[pageCount];
        for (int i = 0; i < pageCount; ++i) {
            Page page = pages.removeFirst();
            pagesToClose[i] = page;
            byteBuffers[i] = page.byteBuffer();
        }

        return new FlushOperation(byteBuffers, (r, e) -> {
            IOUtils.closeWhileHandlingException(pagesToClose);
            listener.accept(r, e);
        });
    }

    boolean hasEncryptedBytesToFlush() {
        return pages.isEmpty() == false;
    }

    @Override
    public void close() {
        IOUtils.closeWhileHandlingException(currentPage);
        IOUtils.closeWhileHandlingException(pages);
    }
}
