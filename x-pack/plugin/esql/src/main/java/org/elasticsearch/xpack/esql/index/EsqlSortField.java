/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.index;

import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Pruning;
import org.elasticsearch.index.ComputedKeySortField;

import java.io.IOException;
import java.util.List;

/**
 * A Lucene index sort on an {@link EsqlSortKey}, which Lucene computes for each document when it sorts a segment at flush and
 * merge time.
 */
public final class EsqlSortField extends ComputedKeySortField {

    private final EsqlSortKey key;

    public EsqlSortField(EsqlSortKey key, boolean reverse, boolean nullsFirst) {
        super(key.expression(), reverse, nullsFirst);
        this.key = key;
    }

    public EsqlSortKey key() {
        return key;
    }

    @Override
    public IndexSorter getIndexSorter() {
        return new Sorter();
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
        throw new UnsupportedOperationException("index sort key [" + getField() + "] can't sort hits");
    }

    /**
     * Sorts on the keys, computed a chunk at a time. A segment that Lucene flushes is read through a {@link ForwardOnlyLeafReader}
     * and its keys are held for all of its documents. Merges read the documents of each segment in order, so only the keys of
     * the current chunk are held.
     */
    private final class Sorter implements IndexSorter {
        @Override
        public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers) throws IOException {
            ComparableProvider[] providers = new ComparableProvider[readers.size()];
            for (int i = 0; i < providers.length; i++) {
                int maxDoc = readers.get(i).maxDoc();
                providers[i] = new ChunkProvider(values(readers.get(i), maxDoc), maxDoc);
            }
            return providers;
        }

        @Override
        public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
            EsqlSortKeyValues values = values(new ForwardOnlyLeafReader(reader, maxDoc), maxDoc);
            long[] keys = new long[maxDoc];
            int doc = 0;
            int count;
            while ((count = values.next(keys, doc)) > 0) {
                doc += count;
            }
            int reverseMul = getReverse() ? -1 : 1;
            return (doc1, doc2) -> reverseMul * Long.compare(keys[doc1], keys[doc2]);
        }

        private EsqlSortKeyValues values(LeafReader reader, int maxDoc) throws IOException {
            return key.values(reader, maxDoc, (Long) getMissingValue());
        }

        @Override
        public String getProviderName() {
            return PROVIDER_NAME;
        }
    }

    /** The keys of a segment whose documents are read in order. */
    private static final class ChunkProvider implements IndexSorter.ComparableProvider {
        private final EsqlSortKeyValues values;
        private final long[] keys;
        private int first;
        private int count;

        ChunkProvider(EsqlSortKeyValues values, int maxDoc) {
            this.values = values;
            this.keys = new long[Math.min(DocValuesExpressionReader.CHUNK, maxDoc)];
        }

        @Override
        public long getAsComparableLong(int doc) throws IOException {
            while (doc >= first + count) {
                first += count;
                count = values.next(keys, 0);
                if (count == 0) {
                    throw new IllegalStateException("document [" + doc + "] is past the last document");
                }
            }
            return keys[doc - first];
        }
    }
}
