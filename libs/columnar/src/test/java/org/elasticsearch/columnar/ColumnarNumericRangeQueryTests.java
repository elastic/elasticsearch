/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * Drives {@link ColumnarNumericRangeQuery} through a real {@link IndexSearcher} over a ColumNAR-coded
 * index and checks the matches against a brute-force scan, over several data shapes, segment sizes and
 * selectivities. The numeric values are written as {@link NumericBinaryPayload} on a tagged binary
 * field — the library's real surface — so the query's vectorized, skipper-aware fast path is exercised.
 */
public class ColumnarNumericRangeQueryTests extends ESTestCase {

    private static final String FIELD = "value";

    public void testRangeQueryMatchesBruteForce() throws IOException {
        for (int iter = 0; iter < 12; iter++) {
            final int numDocs = randomFrom(50, 500, between(2000, 9000));
            final long[] values = new long[numDocs];
            if (randomBoolean()) {
                long base = randomLong() >> 20;
                for (int d = 0; d < numDocs; d++) {
                    base += between(0, 25);
                    values[d] = base;
                }
            } else {
                for (int d = 0; d < numDocs; d++) {
                    values[d] = between(0, 4000);
                }
            }

            try (Directory dir = newDirectory()) {
                indexColumnar(dir, values);
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    for (int q = 0; q < 6; q++) {
                        final long lo;
                        final long hi;
                        switch (between(0, 2)) {
                            case 0 -> {
                                lo = Long.MIN_VALUE;
                                hi = Long.MAX_VALUE; // full domain
                            }
                            case 1 -> {
                                lo = Long.MAX_VALUE;
                                hi = Long.MIN_VALUE; // empty (lo > hi)
                            }
                            default -> {
                                long pivot = values[between(0, numDocs - 1)];
                                int half = between(0, 800);
                                lo = pivot - half;
                                hi = pivot + between(0, half + 1);
                            }
                        }

                        final FixedBitSet expected = new FixedBitSet(numDocs);
                        if (lo <= hi) {
                            for (int d = 0; d < numDocs; d++) {
                                if (values[d] >= lo && values[d] <= hi) {
                                    expected.set(d);
                                }
                            }
                        }

                        final Query query = new ColumnarNumericRangeQuery(FIELD, lo, hi);
                        final FixedBitSet actual = collect(searcher, query, reader.maxDoc());
                        final String msg = "numDocs=" + numDocs + " [" + lo + "," + hi + "]";
                        assertEquals(msg, expected, actual);
                        assertEquals(msg + " count", expected.cardinality(), searcher.count(query));
                    }
                }
            }
        }
    }

    private void indexColumnar(Directory dir, long[] values) throws IOException {
        final Codec codec = columnarCodec();
        final FieldType type = columnarBinaryFieldType(ColumnarFieldType.LONG);
        final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(codec);
        final BytesRefBuilder builder = new BytesRefBuilder();
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (long value : values) {
                final BytesRef payload = BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { value }, 1, builder));
                final Document doc = new Document();
                doc.add(new Field(FIELD, payload, type));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static FixedBitSet collect(IndexSearcher searcher, Query query, int maxDoc) throws IOException {
        final FixedBitSet matches = new FixedBitSet(maxDoc);
        searcher.search(query, new Collector() {
            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }

            @Override
            public LeafCollector getLeafCollector(LeafReaderContext context) {
                final int base = context.docBase;
                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) {
                        matches.set(base + doc);
                    }
                };
            }
        });
        return matches;
    }
}
