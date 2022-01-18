/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.Page;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
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
        int pageCount = pages.size();
        ByteBuffer[] byteBuffers = new ByteBuffer[pageCount];
        Page[] pagesToClose = new Page[pageCount];
        for (int i = 0; i < pageCount; ++i) {
            Page page = pages.removeFirst();
            pagesToClose[i] = page;
            byteBuffers[i] = page.byteBuffer();
        }

        return new FlushOperation(byteBuffers, (r, e) -> {
            try {
                IOUtils.close(pagesToClose);
            } catch (Exception ex) {
                if (e != null) {
                    ex.addSuppressed(e);
                }
                assert false : ex;
                throw new ElasticsearchException(ex);
            }
        });
    }

    boolean hasEncryptedBytesToFlush() {
        return pages.isEmpty() == false;
    }

    @Override
    public void close() {
        Exception closeException = null;
        try {
            IOUtils.close(currentPage);
        } catch (Exception e) {
            closeException = e;
        }
        currentPage = null;
        Page p;
        while ((p = pages.pollFirst()) != null) {
            try {
                p.close();
            } catch (Exception ex) {
                closeException = ExceptionsHelper.useOrSuppress(closeException, ex);
            }
        }
        if (closeException != null) {
            assert false : closeException;
            throw new ElasticsearchException(closeException);
        }
    }
}
