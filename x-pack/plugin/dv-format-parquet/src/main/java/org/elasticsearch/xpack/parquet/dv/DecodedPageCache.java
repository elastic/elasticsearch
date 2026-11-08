/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The decoded pages that the forward scans of a segment produced, by field and first document. A page holds values that the ES|QL
 * reader already decompressed and decoded, not file bytes, so this cache saves decoding work inside a query and not reads from the
 * repository (a disk cache of file ranges would sit below the reader and is a separate layer).
 *
 * <p>Why it exists: a query reads one column through several live iterators on one thread, namely the scorers of its filter and the
 * iterator that loads the values, and they read the same pages almost in lockstep. In Parquet a filter such as {@code SearchPhrase != ""}
 * has no shortcut (there is no ordinal or statistics answer at page level), so each of those iterators decoded every page for itself and
 * the cost of reading a column grew with the number of iterators. The iterator that gets to a page first now publishes it and the
 * others find it here, which took {@code q12} of the ClickBench set from about 9 s to 5 s on the prototype.
 *
 * <p>The cache is bounded in bytes and drops the pages that were added first, because the scans move forward and the oldest pages are
 * the ones that no iterator comes back to. It belongs to one segment and goes away with it. Note that for a column larger than the bound
 * it keeps only the most recent pages, so it helps the iterators of one query and typically not the next query.
 *
 * <p>Why this is not a generic cache: we measured the same pattern on the server's own formats (ColumNAR strings and the time series
 * format's binary doc values, 4 million ClickBench rows, one segment) by counting how often a block is decompressed again. Most
 * queries repeat only 3 to 5% of their block loads, which come from slice boundaries, and the one visible case is a filter that needs the
 * bytes followed by a load of the same column ({@code q22}, 29% repeats, at best about 17% of that query's CPU). A filter such as
 * {@code SearchPhrase != ""} needed almost no block loads of its own there, so sharing does not pay. The place to share decoded blocks
 * would be the doc values producer of a format, which knows its block boundaries, and not ES|QL, whose operators only see the
 * iterators. These counts varied between runs, so treat them as indicative.
 */
final class DecodedPageCache {

    private record Entry(String field, int start, ColumnPage page) {}

    private final Map<String, ConcurrentSkipListMap<Integer, ColumnPage>> byField = new ConcurrentHashMap<>();
    private final Queue<Entry> insertionOrder = new ConcurrentLinkedQueue<>();
    private final AtomicLong weight = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong published = new AtomicLong();
    private final long maxWeight;

    DecodedPageCache(long maxWeight) {
        this.maxWeight = maxWeight;
    }

    /** The page of the field that holds the document, or {@code null}. */
    @Nullable
    ColumnPage find(String field, int doc) {
        ConcurrentSkipListMap<Integer, ColumnPage> pages = byField.get(field);
        if (pages == null) {
            return null;
        }
        Map.Entry<Integer, ColumnPage> floor = pages.floorEntry(doc);
        if (floor != null && floor.getValue().contains(doc)) {
            hits.incrementAndGet();
            return floor.getValue();
        }
        return null;
    }

    /** Publishes a page, unless one that starts at the same document is there already, and drops the oldest pages beyond the bound. */
    void put(String field, ColumnPage page) {
        ConcurrentSkipListMap<Integer, ColumnPage> pages = byField.computeIfAbsent(field, f -> new ConcurrentSkipListMap<>());
        if (pages.putIfAbsent(page.start(), page) != null) {
            return;
        }
        published.incrementAndGet();
        insertionOrder.add(new Entry(field, page.start(), page));
        long total = weight.addAndGet(page.weight());
        while (total > maxWeight) {
            Entry oldest = insertionOrder.poll();
            if (oldest == null) {
                break;
            }
            ConcurrentSkipListMap<Integer, ColumnPage> map = byField.get(oldest.field());
            if (map != null && map.remove(oldest.start(), oldest.page())) {
                total = weight.addAndGet(-oldest.page().weight());
            }
        }
    }

    void clear() {
        byField.clear();
        insertionOrder.clear();
        weight.set(0);
    }

    long hits() {
        return hits.get();
    }

    long published() {
        return published.get();
    }

    /**
     * The decoded values of a run of consecutive documents of one field. A page is immutable once it is built, so the iterators of a
     * segment share it without a lock: whichever iterator decodes a page first publishes it in the {@link DecodedPageCache}, and the others
     * read it.
     */
    abstract static class ColumnPage {

        private final int start;
        private final int count;
        /** {@code present[i]} tells whether document {@code start + i} has a value, or {@code null} when every document has one. */
        @Nullable
        private final boolean[] present;

        ColumnPage(int start, int count, @Nullable boolean[] present) {
            this.start = start;
            this.count = count;
            this.present = present;
        }

        final int start() {
            return start;
        }

        /** The first document after the page. */
        final int end() {
            return start + count;
        }

        final boolean contains(int doc) {
            return doc >= start && doc < start + count;
        }

        /** Whether the document, which must be in the page, has a value. */
        final boolean has(int doc) {
            return present == null || present[doc - start];
        }

        /** The first document at or after {@code from} in this page that has a value, or {@link DocIdSetIterator#NO_MORE_DOCS}. */
        final int nextPresent(int from) {
            if (present == null) {
                return from < end() ? from : DocIdSetIterator.NO_MORE_DOCS;
            }
            for (int i = from - start; i < count; i++) {
                if (present[i]) {
                    return start + i;
                }
            }
            return DocIdSetIterator.NO_MORE_DOCS;
        }

        /** The approximate size of the page in bytes, which the cache bounds. */
        final long weight() {
            return 64 + (present == null ? 0 : count) + valuesWeight();
        }

        abstract long valuesWeight();
    }
}
