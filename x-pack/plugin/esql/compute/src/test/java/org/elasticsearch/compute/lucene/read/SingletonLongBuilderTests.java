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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.SingleArrayPool;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;
import org.elasticsearch.indices.CrankyCircuitBreakerService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

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
                    try (SingletonLongBuilder builder = new SingletonLongBuilder(ctx.reader().numDocs(), factory, null)) {
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
                try (SingletonLongBuilder builder = new SingletonLongBuilder(count - offset, blockFactory(), null)) {
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

    public void testNulls() {
        BlockFactory factory = blockFactory();
        try (SingleArrayPool pool = new SingleArrayPool(factory.breaker(), () -> {})) {
            for (int page = 0; page < 3; page++) {
                int count = between(1, 100);
                boolean[] nulls = new boolean[count];
                try (SingletonLongBuilder builder = new SingletonLongBuilder(count, factory, randomBoolean() ? pool : null)) {
                    for (int i = 0; i < count; i++) {
                        nulls[i] = page != 1 && randomBoolean();
                        if (nulls[i]) {
                            builder.appendNull();
                        } else {
                            builder.appendLong(i);
                        }
                    }
                    try (LongBlock block = (LongBlock) builder.build()) {
                        assertThat(block.getPositionCount(), equalTo(count));
                        for (int i = 0; i < count; i++) {
                            assertThat(block.isNull(i), equalTo(nulls[i]));
                            if (nulls[i] == false) {
                                assertThat(block.getLong(block.getFirstValueIndex(i)), equalTo((long) i));
                            }
                        }
                    }
                }
            }
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * A field that is missing from some documents is read with nulls, and still pools its array.
     */
    public void testReadSparsePoolsArray() throws IOException {
        BlockFactory factory = blockFactory();
        int count = 1_000;
        try (Directory directory = newDirectory(); SingleArrayPool pool = new SingleArrayPool(factory.breaker(), () -> {})) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    indexWriter.addDocument(i % 3 == 0 ? List.of() : List.of(new NumericDocValuesField("field", i)));
                }
                indexWriter.forceMerge(1);
            }
            try (
                IndexReader reader = DirectoryReader.open(directory);
                ComputeBlockLoaderFactory loaderFactory = new ComputeBlockLoaderFactory(factory)
            ) {
                LeafReaderContext ctx = reader.leaves().get(0);
                try (BlockLoader.ColumnAtATimeReader columnReader = new LongsBlockLoader("field").reader(factory.breaker(), ctx)) {
                    long readerOnly = factory.breaker().getUsed();
                    for (int from = 0; from < count; from += 250) {
                        int[] docs = IntStream.range(from, from + 250).toArray();
                        try (LongBlock block = (LongBlock) columnReader.read(loaderFactory, TestBlock.docs(docs), 0, false, pool)) {
                            assertThat(block.getPositionCount(), equalTo(docs.length));
                            for (int i = 0; i < docs.length; i++) {
                                assertThat(block.isNull(i), equalTo(docs[i] % 3 == 0));
                                if (block.isNull(i) == false) {
                                    assertThat(block.getLong(block.getFirstValueIndex(i)), equalTo((long) docs[i]));
                                }
                            }
                        }
                        assertThat(factory.breaker().getUsed() - readerOnly, greaterThan(0L));
                    }
                }
            }
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testPoolBreaks() {
        BlockFactory factory = blockFactory(ByteSizeValue.ofBytes(1000));
        try (SingleArrayPool pool = new SingleArrayPool(factory.breaker(), () -> {})) {
            expectThrows(CircuitBreakingException.class, () -> new SingletonLongBuilder(100, factory, pool));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    /**
     * Reads pages through the real {@link LongsBlockLoader} reader. The pool given to
     * {@code read} keeps its array charged between pages and releases it on close.
     */
    public void testReadPoolsArray() throws IOException {
        BlockFactory factory = blockFactory();
        try (SingleArrayPool pool = new SingleArrayPool(factory.breaker(), () -> {})) {
            assertThat(readPages(factory, pool), greaterThan(0L));
        }
        assertThat(factory.breaker().getUsed(), equalTo(0L));
    }

    public void testReadWithoutPool() throws IOException {
        assertThat(readPages(blockFactory(), null), equalTo(0L));
    }

    /**
     * Reads two pages.
     * @return the bytes held on to between them
     */
    private long readPages(BlockFactory factory, BlockLoader.ArrayPool pool) throws IOException {
        int count = 1_000;
        long pooled;
        try (Directory directory = newDirectory()) {
            try (IndexWriter indexWriter = createIndexWriter(directory)) {
                for (int i = 0; i < count; i++) {
                    indexWriter.addDocument(List.of(new NumericDocValuesField("field", i)));
                }
                indexWriter.forceMerge(1);
            }
            try (
                IndexReader reader = DirectoryReader.open(directory);
                ComputeBlockLoaderFactory loaderFactory = new ComputeBlockLoaderFactory(factory)
            ) {
                LeafReaderContext ctx = reader.leaves().get(0);
                long empty = factory.breaker().getUsed();
                try (BlockLoader.ColumnAtATimeReader columnReader = new LongsBlockLoader("field").reader(factory.breaker(), ctx)) {
                    long readerOnly = factory.breaker().getUsed();
                    readPage(columnReader, loaderFactory, pool, 0, 500);
                    pooled = factory.breaker().getUsed() - readerOnly;
                    readPage(columnReader, loaderFactory, pool, 500, count);
                    assertThat(factory.breaker().getUsed() - readerOnly, equalTo(pooled));
                }
                assertThat(factory.breaker().getUsed() - empty, equalTo(pooled));
            }
        }
        return pooled;
    }

    private void readPage(
        BlockLoader.ColumnAtATimeReader reader,
        ComputeBlockLoaderFactory factory,
        BlockLoader.ArrayPool pool,
        int from,
        int to
    ) throws IOException {
        int[] docs = IntStream.range(from, to).toArray();
        try (LongBlock block = (LongBlock) reader.read(factory, TestBlock.docs(docs), 0, false, pool)) {
            assertThat(block.getPositionCount(), equalTo(docs.length));
            for (int i = 0; i < docs.length; i++) {
                assertThat(block.getLong(i), equalTo((long) docs[i]));
            }
        }
    }

    static IndexWriter createIndexWriter(Directory directory) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
        iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819Version3TSDBDocValuesFormat()));
        return new IndexWriter(directory, iwc);
    }

}
