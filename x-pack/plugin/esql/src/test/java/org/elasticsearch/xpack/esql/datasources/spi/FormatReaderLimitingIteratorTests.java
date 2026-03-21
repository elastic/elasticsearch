/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

public class FormatReaderLimitingIteratorTests extends ESTestCase {

    public void testStopsAtLimit() throws IOException {
        List<Page> pages = List.of(page(10), page(10), page(10));
        try (var iter = new FormatReader.LimitingIterator(listIterator(pages), 25)) {
            int totalRows = 0;
            int pageCount = 0;
            while (iter.hasNext()) {
                Page p = iter.next();
                totalRows += p.getPositionCount();
                pageCount++;
            }
            // First two pages: 10+10=20, third page trimmed from 10 to 5
            assertEquals(25, totalRows);
            assertEquals(3, pageCount);
        }
    }

    public void testStopsBeforeExhaustingIterator() throws IOException {
        List<Page> pages = List.of(page(100), page(100), page(100));
        try (var iter = new FormatReader.LimitingIterator(listIterator(pages), 50)) {
            assertTrue(iter.hasNext());
            Page p = iter.next();
            // Page trimmed from 100 to 50
            assertEquals(50, p.getPositionCount());
            assertFalse(iter.hasNext());
        }
    }

    public void testClosesDelegate() throws IOException {
        AtomicBoolean closed = new AtomicBoolean(false);
        CloseableIterator<Page> delegate = new CloseableIterator<>() {
            private int remaining = 3;

            @Override
            public boolean hasNext() {
                return remaining > 0;
            }

            @Override
            public Page next() {
                remaining--;
                return page(100);
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        try (var iter = new FormatReader.LimitingIterator(delegate, 50)) {
            Page p = iter.next(); // 100 rows trimmed to 50, budget exhausted -> delegate closed
            assertEquals(50, p.getPositionCount());
            assertTrue(closed.get());
        }
    }

    public void testEmptyIterator() throws IOException {
        List<Page> pages = List.of();
        try (var iter = new FormatReader.LimitingIterator(listIterator(pages), 10)) {
            assertFalse(iter.hasNext());
            expectThrows(NoSuchElementException.class, iter::next);
        }
    }

    public void testExactLimit() throws IOException {
        List<Page> pages = List.of(page(5), page(5));
        try (var iter = new FormatReader.LimitingIterator(listIterator(pages), 10)) {
            int totalRows = 0;
            while (iter.hasNext()) {
                totalRows += iter.next().getPositionCount();
            }
            assertEquals(10, totalRows);
        }
    }

    public void testRejectsNonPositiveLimit() {
        expectThrows(IllegalArgumentException.class, () -> new FormatReader.LimitingIterator(listIterator(List.of()), 0));
        expectThrows(IllegalArgumentException.class, () -> new FormatReader.LimitingIterator(listIterator(List.of()), -1));
    }

    private static Page page(int positionCount) {
        return new Page(positionCount);
    }

    private static CloseableIterator<Page> listIterator(List<Page> pages) {
        return new CloseableIterator<>() {
            private final List<Page> list = new ArrayList<>(pages);
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < list.size();
            }

            @Override
            public Page next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                return list.get(index++);
            }

            @Override
            public void close() {}
        };
    }
}
