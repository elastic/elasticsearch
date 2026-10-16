/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.lucene.store.PrefetchableDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Prefetches the documents a consumer reads across many doc-values columns and segments. The consumer adds each segment,
 * in reading order, with its documents and the doc-values fields it reads; several readers can add fields to the same
 * segment.
 *
 * <p>Documents are prefetched a window at a time. A window starts at the first document not covered yet and runs into the
 * following segments while the budget of document and column pairs lasts. Each stage runs on every column of the window
 * before the next; a document with more columns than the budget takes them a group at a time.
 */
public final class DocValuesBatchPrefetcher {

    /** The documents to read from one segment and the columns they are read from. */
    public static final class Segment {
        private final LeafReader reader;
        private final int[] docs;
        private final List<PrefetchableDocValues> columns = new ArrayList<>();
        private final Set<String> fields = new HashSet<>();
        private boolean[] pending = new boolean[0];
        /** One past the last position of {@link #docs} that a window has covered. */
        private int covered = 0;
        private int windowFrom;

        private Segment(LeafReader reader, int[] docs) {
            this.reader = reader;
            this.docs = docs;
        }

        /** The documents to read from the segment, in ascending order. */
        public int[] docs() {
            return docs;
        }

        /** Adds the columns of {@code fields} that can be prefetched, each opened separately from the one the consumer reads. */
        public void addFields(Collection<String> fields) throws IOException {
            for (String field : fields) {
                if (this.fields.add(field) == false) {
                    continue;
                }
                FieldInfo info = reader.getFieldInfos().fieldInfo(field);
                if (info == null) {
                    continue;
                }
                Object column = switch (info.getDocValuesType()) {
                    case NONE -> null;
                    case NUMERIC -> reader.getNumericDocValues(field);
                    case BINARY -> reader.getBinaryDocValues(field);
                    case SORTED -> reader.getSortedDocValues(field);
                    case SORTED_NUMERIC -> {
                        var values = reader.getSortedNumericDocValues(field);
                        var singleton = DocValues.unwrapSingleton(values);
                        yield singleton != null ? singleton : values;
                    }
                    case SORTED_SET -> {
                        var values = reader.getSortedSetDocValues(field);
                        var singleton = DocValues.unwrapSingleton(values);
                        yield singleton != null ? singleton : values;
                    }
                };
                if (column instanceof PrefetchableDocValues prefetchable) {
                    columns.add(prefetchable);
                }
            }
        }
    }

    private final int pairsPerRound;
    private final int readUnit;
    private final List<Segment> segments = new ArrayList<>();
    private final List<Segment> window = new ArrayList<>();

    /**
     * @param budget   the document and column pairs a window holds
     * @param readUnit see {@link PrefetchableDocValues#prefetch}
     */
    public DocValuesBatchPrefetcher(int budget, int readUnit) {
        this.pairsPerRound = Math.max(1, budget);
        this.readUnit = readUnit;
    }

    /** The segment of {@code reader} with {@code docs}, added after the others unless it was already. */
    public Segment segment(LeafReader reader, int[] docs) {
        for (Segment segment : segments) {
            if (segment.reader == reader && Arrays.equals(segment.docs, docs)) {
                return segment;
            }
        }
        Segment segment = new Segment(reader, docs);
        segments.add(segment);
        return segment;
    }

    /**
     * Makes sure the document at {@code index} of {@code segment} was prefetched, opening a window there if needed.
     *
     * @return one past the last position of the segment's documents that has been prefetched
     */
    public int cover(Segment segment, int index) throws IOException {
        if (index < segment.covered) {
            return segment.covered;
        }
        window.clear();
        int remaining = pairsPerRound;
        int from = index;
        for (int s = segments.indexOf(segment); s < segments.size(); s++) {
            Segment next = segments.get(s);
            if (next != segment) {
                from = next.covered;
            }
            int available = next.docs.length - from;
            if (available == 0) {
                continue;
            }
            int perDoc = Math.max(1, next.columns.size());
            int count = Math.min(available, remaining / perDoc);
            if (count <= 0) {
                if (window.isEmpty() == false) {
                    break;
                }
                // A window holds at least one document, even one with more columns than the budget.
                count = 1;
            }
            next.windowFrom = from;
            next.covered = from + count;
            if (next.pending.length < next.columns.size()) {
                next.pending = new boolean[next.columns.size()];
            }
            window.add(next);
            remaining -= count * perDoc;
            if (count < available) {
                break;
            }
        }
        prefetchWindow();
        return segment.covered;
    }

    /** Runs every stage over the window, a group of at most the budget's worth of columns at a time. */
    private void prefetchWindow() throws IOException {
        int groupFirstSegment = 0;
        int groupFirstColumn = 0;
        while (groupFirstSegment < window.size()) {
            // Find where the group ends: after pairsPerRound columns, or at the end of the window.
            int lastSegment = groupFirstSegment;
            int lastColumn = groupFirstColumn;
            int taken = 0;
            while (lastSegment < window.size() && taken < pairsPerRound) {
                int available = window.get(lastSegment).columns.size() - lastColumn;
                int take = Math.min(available, pairsPerRound - taken);
                taken += take;
                lastColumn += take;
                if (lastColumn == window.get(lastSegment).columns.size()) {
                    lastSegment++;
                    lastColumn = 0;
                }
            }
            boolean more = true;
            for (int stage = 0; more; stage++) {
                more = false;
                for (int s = groupFirstSegment; s <= lastSegment && s < window.size(); s++) {
                    Segment segment = window.get(s);
                    int first = s == groupFirstSegment ? groupFirstColumn : 0;
                    int end = s == lastSegment ? lastColumn : segment.columns.size();
                    for (int c = first; c < end; c++) {
                        if (stage == 0 || segment.pending[c]) {
                            segment.pending[c] = segment.columns.get(c)
                                .prefetch(stage, segment.docs, segment.windowFrom, segment.covered, readUnit);
                            more |= segment.pending[c];
                        }
                    }
                }
            }
            groupFirstSegment = lastSegment;
            groupFirstColumn = lastColumn;
        }
    }
}
