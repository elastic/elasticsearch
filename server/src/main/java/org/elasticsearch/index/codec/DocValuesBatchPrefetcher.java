/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.IOConsumer;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.lucene.store.PrefetchableDocValues;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Prefetches the documents a consumer reads across many doc-values columns and segments. The consumer adds each segment,
 * in reading order, with its documents and the doc-values fields it reads; several readers can add fields to the same
 * segment.
 *
 * <p>Documents are prefetched a window at a time. A window starts at the first document not covered yet and runs into the
 * following segments while the budget of document and column pairs lasts. Each stage runs on every column of the window
 * before the next. A window holds at least one document; when it has more columns than the budget, they are prefetched
 * a budget's worth at a time.
 */
public final class DocValuesBatchPrefetcher {

    /** The documents to read from one segment and the columns they are read from. */
    public static final class Segment {
        private final LeafReader reader;
        private final int[] docs;
        private final List<PrefetchableDocValues> columns = new ArrayList<>();
        private final Set<String> fields = new HashSet<>();
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

        /**
         * Adds the columns of the doc-values fields {@code open} opens on the segment, without reading them: a consumer
         * announces what it reads by opening it as it would to read.
         */
        public void addFieldsOpenedBy(IOConsumer<LeafReader> open) throws IOException {
            FieldRecorder recorder = new FieldRecorder(reader);
            open.accept(recorder);
            addFields(recorder.fields);
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

    /**
     * The document and doc-values column pairs a consumer prefetches at once. {@code 0}, the default, turns doc-values
     * prefetching off for every consumer. Only an operator can change it.
     */
    public static final Setting<Integer> BUDGET_SETTING = Setting.intSetting(
        "search.doc_values_prefetch.budget",
        0,
        0,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    private final int budget;
    private final List<Segment> segments = new ArrayList<>();
    private final List<Segment> window = new ArrayList<>();

    /** @param budget the document and column pairs a window holds */
    public DocValuesBatchPrefetcher(int budget) {
        this.budget = Math.max(1, budget);
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
        int remaining = budget;
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
            window.add(next);
            remaining -= count * perDoc;
            if (count < available) {
                break;
            }
        }
        prefetchWindow();
        return segment.covered;
    }

    /** A column of a segment of the window. */
    private record Column(Segment segment, PrefetchableDocValues values) {}

    /** Runs every stage over the columns of the window, a budget's worth of columns at a time. */
    private void prefetchWindow() throws IOException {
        List<Column> columns = new ArrayList<>();
        for (Segment segment : window) {
            for (PrefetchableDocValues values : segment.columns) {
                columns.add(new Column(segment, values));
            }
        }
        for (int start = 0; start < columns.size(); start += budget) {
            List<Column> group = columns.subList(start, Math.min(start + budget, columns.size()));
            boolean[] pending = new boolean[group.size()];
            Arrays.fill(pending, true);
            boolean more = true;
            for (int stage = 0; more; stage++) {
                more = false;
                for (int c = 0; c < group.size(); c++) {
                    if (pending[c]) {
                        Segment segment = group.get(c).segment();
                        pending[c] = group.get(c).values().prefetch(stage, segment.docs, segment.windowFrom, segment.covered);
                        more |= pending[c];
                    }
                }
            }
        }
    }

    /** Records the doc-values fields opened on a segment. */
    private static final class FieldRecorder extends FilterLeafReader {
        private final Set<String> fields = new LinkedHashSet<>();

        FieldRecorder(LeafReader in) {
            super(in);
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            fields.add(field);
            return in.getNumericDocValues(field);
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) throws IOException {
            fields.add(field);
            return in.getBinaryDocValues(field);
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) throws IOException {
            fields.add(field);
            return in.getSortedDocValues(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
            fields.add(field);
            return in.getSortedNumericDocValues(field);
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
            fields.add(field);
            return in.getSortedSetDocValues(field);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
