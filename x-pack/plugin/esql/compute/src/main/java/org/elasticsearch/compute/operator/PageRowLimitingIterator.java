/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Page;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Wrapper around {@link CloseableIterator} of {@link Page}s that stops yielding
 * pages once a cumulative row budget is exhausted. When the last page would overshoot
 * the budget, it is trimmed to the exact remaining count.
 */
public class PageRowLimitingIterator implements CloseableIterator<Page> {
    private final CloseableIterator<Page> delegate;
    private int remaining;

    public PageRowLimitingIterator(CloseableIterator<Page> delegate, int rowLimit) {
        if (rowLimit <= 0) {
            throw new IllegalArgumentException("rowLimit must be positive, got: " + rowLimit);
        }
        this.delegate = delegate;
        this.remaining = rowLimit;
    }

    @Override
    public boolean hasNext() {
        if (remaining <= 0) {
            return false;
        }
        return delegate.hasNext();
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        Page page = delegate.next();
        int rows = page.getPositionCount();
        if (rows > remaining) {
            page = truncate(page, remaining);
            remaining = 0;
        } else {
            remaining -= rows;
        }
        if (remaining <= 0) {
            try {
                delegate.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return page;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private static Page truncate(Page page, int upTo) {
        try {
            return page.slice(0, upTo);
        } finally {
            page.releaseBlocks();
        }
    }
}
