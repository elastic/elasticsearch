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
import org.elasticsearch.compute.data.LongVector;
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

public class SingletonLongBuilderTests extends ComputeTestCase {

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
        Long[] values = new Long[] { 1L, 2L, 3L, 4L };

        int count = 1000;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    Long v = values[i % values.length];
                    indexWriter.addDocument(List.of(new NumericDocValuesField("field", v)));
                }
            }
            Map<Long, Integer> counts = new HashMap<>();
            try (IndexReader reader = DirectoryReader.open(directory)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    var docValues = ctx.reader().getNumericDocValues("field");
                    try (SingletonLongBuilder builder = new SingletonLongBuilder(ctx.reader().numDocs(), factory)) {
                        for (int i = 0; i < ctx.reader().maxDoc(); i++) {
                            assertThat(docValues.advanceExact(i), equalTo(true));
                            long value = docValues.longValue();
                            if (randomBoolean()) {
                                builder.appendLongs(new long[] { value }, 0, 1);
                            } else {
                                builder.appendLong(value);
                            }
                        }
                        try (LongVector build = (LongVector) builder.build().asVector()) {
                            for (int i = 0; i < build.getPositionCount(); i++) {
                                long key = build.getLong(i);
                                counts.merge(key, 1, Integer::sum);
                            }
                        }
                    }
                }
            }
            int expectedCount = count / values.length;
            assertMap(
                counts,
                matchesMap().entry(1L, expectedCount).entry(2L, expectedCount).entry(3L, expectedCount).entry(4L, expectedCount)
            );
        }
    }

    public void testMoreValues() throws IOException {
        int count = 1_000;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    indexWriter.addDocument(List.of(new NumericDocValuesField("field", i)));
                }
                indexWriter.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertThat(reader.leaves().size(), equalTo(1));
                LeafReader leafReader = reader.leaves().get(0).reader();
                var docValues = leafReader.getNumericDocValues("field");
                int offset = 850;
                try (SingletonLongBuilder builder = new SingletonLongBuilder(count - offset, blockFactory())) {
                    for (int i = offset; i < leafReader.maxDoc(); i++) {
                        assertThat(docValues.advanceExact(i), equalTo(true));
                        long value = docValues.longValue();
                        if (randomBoolean()) {
                            builder.appendLongs(new long[] { value }, 0, 1);
                        } else {
                            builder.appendLong(value);
                        }
                    }
                    try (LongVector build = (LongVector) builder.build().asVector()) {
                        assertThat(build.getPositionCount(), equalTo(count - offset));
                        for (int i = 0; i < build.getPositionCount(); i++) {
                            Long key = build.getLong(i);
                            assertThat(key, equalTo((long) offset + i));
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
