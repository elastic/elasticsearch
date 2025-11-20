/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;

public class SingletonDoubleBuilderTests extends ComputeTestCase {

    public void testReader() throws IOException {
        testRead(blockFactory());
    }

    public void testReadWithCranky() throws IOException {
        var factory = crankyBlockFactory();
        try {
            testRead(factory);
            // If we made it this far cranky didn't fail us!
        } catch (CircuitBreakingException e) {
            logger.info("cranky", e);
            assertThat(e.getMessage(), equalTo(CrankyCircuitBreakerService.ERROR_MESSAGE));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    private void testRead(BlockFactory factory) throws IOException {
        Double[] values = new Double[] { 1.1, 2.2, 3.3, 4.4 };

        int count = 1000;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    double v = values[i % values.length];
                    indexWriter.addDocument(List.of(new NumericDocValuesField("field", Double.doubleToRawLongBits(v))));
                }
            }
            Map<Double, Integer> counts = new HashMap<>();
            try (IndexReader reader = DirectoryReader.open(directory)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    var docValues = ctx.reader().getNumericDocValues("field");
                    try (var builder = new SingletonDoubleBuilder(ctx.reader().numDocs(), factory)) {
                        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                            assertThat(docValues.advanceExact(i), equalTo(true));
                            builder.appendLongs(Double::longBitsToDouble, new long[] { docValues.longValue() }, 0, 1);
                        }
                        try (var build = (DoubleVector) builder.build().asVector()) {
                            for (int i = 0; i < build.getPositionCount(); i++) {
                                double key = build.getDouble(i);
                                counts.merge(key, 1, Integer::sum);
                            }
                        }
                    }
                }
            }
            int expectedCount = count / values.length;
            assertMap(
                counts,
                matchesMap().entry(values[0], expectedCount)
                    .entry(values[1], expectedCount)
                    .entry(values[2], expectedCount)
                    .entry(values[3], expectedCount)
            );
        }
    }

    public void testMoreValues() throws IOException {
        int count = 1_000;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    indexWriter.addDocument(List.of(new NumericDocValuesField("field", Double.doubleToRawLongBits(i / (double) count))));
                }
                indexWriter.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(1));
                LeafReader leafReader = reader.leaves().get(0).reader();
                var docValues = leafReader.getNumericDocValues("field");
                int offset = 850;
                try (var builder = new SingletonDoubleBuilder(count - offset, blockFactory())) {
                    for (int i = offset; i < leafReader.maxDoc(); i++) {
                        assertThat(docValues.advanceExact(i), equalTo(true));
                        builder.appendLongs(Double::longBitsToDouble, new long[] { docValues.longValue() }, 0, 1);
                    }
                    try (var build = (DoubleVector) builder.build().asVector()) {
                        assertThat(build.getPositionCount(), equalTo(count - offset));
                        for (int i = 0; i < build.getPositionCount(); i++) {
                            double key = build.getDouble(i);
                            assertThat(key, equalTo((offset + i) / (double) count));
                        }
                    }
                }
            }
        }
    }

    static IndexWriter createIndexWriter(Directory directory) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
        return new IndexWriter(directory, iwc);
    }

}
