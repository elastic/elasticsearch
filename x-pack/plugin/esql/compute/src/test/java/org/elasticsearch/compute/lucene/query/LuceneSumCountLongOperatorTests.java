/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromList;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.blockloader.docvalues.LongsBlockLoader;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests {@link LuceneSumCountLongOperator} against the exact sum of the values, with the doc values of a default codec, which are
 * read one value at a time, and with the ones of the TSDB format, which hand their decoded blocks over.
 */
public class LuceneSumCountLongOperatorTests extends ComputeTestCase {
    private static final String FIELD = "v";

    public void testEveryDocIsMatched() throws IOException {
        Counts counts = run(new Index(between(1, 20_000)), Queries.ALL_DOCS_INSTANCE);
        // Nothing to ask the doc ids for.
        assertThat(counts.docsWithIds, equalTo(0L));
        assertThat(counts.docsWithoutIds, greaterThan(0L));
    }

    public void testSmallIndexes() throws IOException {
        // Around the 128 values of a block of the TSDB format and the 4 values a loop of the sum is unrolled by.
        for (int numDocs : new int[] { 1, 2, 3, 4, 5, 127, 128, 129, 255, 256, 257 }) {
            run(new Index(numDocs), Queries.ALL_DOCS_INSTANCE);
        }
    }

    public void testExtremeValues() throws IOException {
        Index index = new Index(between(1000, 5000));
        index.extremeValues = true;
        run(index, Queries.ALL_DOCS_INSTANCE);
    }

    public void testSomeDocsDontHaveTheField() throws IOException {
        Index index = new Index(between(1, 20_000));
        index.sparse = true;
        run(index, Queries.ALL_DOCS_INSTANCE);
    }

    public void testDeletedDocsAreSkipped() throws IOException {
        Index index = new Index(between(10, 20_000));
        index.deletes = true;
        Counts counts = run(index, Queries.ALL_DOCS_INSTANCE);
        assertThat("with deletes the doc ids have to be looked at", counts.docsWithIds, greaterThan(0L));
    }

    public void testQuery() throws IOException {
        Counts counts = run(new Index(between(10, 20_000)), new TermQuery(new Term("kw", "a")));
        assertThat(counts.docsWithIds, greaterThan(0L));
    }

    public void testQueryWithDeletesAndSparseField() throws IOException {
        Index index = new Index(between(10, 20_000));
        index.deletes = true;
        index.sparse = true;
        run(index, new TermQuery(new Term("kw", "a")));
    }

    public void testNothingMatches() throws IOException {
        Counts counts = run(new Index(between(1, 2000)), new TermQuery(new Term("kw", "missing")));
        assertThat(counts.count, equalTo(0L));
    }

    /**
     * Shards with different doc values formats, and one that doesn't have the field at all, are added up together. In each of the
     * first two a whole segment doesn't have the field either: it has no doc values to read, which is a constant null.
     */
    public void testShardsOfDifferentFormatsAndOneWithoutTheField() throws IOException {
        Expected expected = new Expected();
        List<Directory> directories = new ArrayList<>();
        List<IndexReader> readers = new ArrayList<>();
        try {
            for (DocValuesFormat format : new DocValuesFormat[] { null, new ES819TSDBDocValuesFormat(), new ES819TSDBDocValuesFormat(9) }) {
                Directory directory = newDirectory();
                directories.add(directory);
                readers.add(indexShard(directory, format, between(1, 6000), true, expected));
            }
            // A shard with documents, but not a single value of the field
            Directory withoutField = newDirectory();
            directories.add(withoutField);
            readers.add(indexShard(withoutField, new ES819TSDBDocValuesFormat(), between(1, 3000), false, expected));

            Counts counts = runOperator(readers, Queries.ALL_DOCS_INSTANCE, expected.sum, expected.count, "shards=" + readers.size());
            assertThat(counts.count, equalTo(expected.count));
        } finally {
            IOUtils.close(readers);
            IOUtils.close(directories);
        }
    }

    /** One shard that is all but empty of the field, so that the sum and the count are those of the other. */
    public void testNothingButAShardWithoutTheField() throws IOException {
        Expected expected = new Expected();
        try (Directory directory = newDirectory()) {
            IndexReader reader = indexShard(directory, new ES819TSDBDocValuesFormat(), between(1, 3000), false, expected);
            try {
                Counts counts = runOperator(List.of(reader), Queries.ALL_DOCS_INSTANCE, BigInteger.ZERO, 0, "without the field");
                assertThat(counts.count, equalTo(0L));
            } finally {
                reader.close();
            }
        }
    }

    private static class Expected {
        BigInteger sum = BigInteger.ZERO;
        long count;
    }

    /**
     * Indexes a shard into {@code directory}: a segment of documents without the field, and then, if {@code withField}, one with
     * it. They stay two segments, so one of them has no doc values for the field to read at all.
     */
    private IndexReader indexShard(Directory directory, DocValuesFormat format, int numDocs, boolean withField, Expected expected)
        throws IOException {
        IndexWriterConfig config = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        if (format != null) {
            config.setCodec(TestUtil.alwaysDocValuesFormat(format));
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int d = between(0, 200); d > 0; d--) {
                Document doc = new Document();
                doc.add(new StringField("kw", "a", StringField.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            for (int d = 0; d < numDocs; d++) {
                Document doc = new Document();
                doc.add(new StringField("kw", "a", StringField.Store.NO));
                if (withField) {
                    long value = randomBoolean() ? randomLong() : randomLongBetween(-1000, 1000);
                    doc.add(new NumericDocValuesField(FIELD, value));
                    expected.sum = expected.sum.add(BigInteger.valueOf(value));
                    expected.count++;
                }
                writer.addDocument(doc);
            }
        }
        return DirectoryReader.open(directory);
    }

    private static class Index {
        final int numDocs;
        boolean sparse;
        boolean deletes;
        boolean extremeValues;

        Index(int numDocs) {
            this.numDocs = numDocs;
        }
    }

    private record Counts(long count, long docsWithoutIds, long docsWithIds) {}

    private Counts run(Index index, Query query) throws IOException {
        // The default doc values, the TSDB ones that decode 128 values at a time, and the larger blocks of the time series indices.
        String docValues = randomFrom("default", "tsdb", "tsdb-large-blocks");
        long[] values = new long[index.numDocs];
        boolean[] hasValue = new boolean[index.numDocs];
        boolean[] deleted = new boolean[index.numDocs];
        IndexWriterConfig config = newIndexWriterConfig();
        switch (docValues) {
            case "default" -> {
            }
            case "tsdb" -> config.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
            // 2^9 values, which is what ES819TSDBDocValuesFormat.NUMERIC_LARGE_BLOCK_SHIFT is
            case "tsdb-large-blocks" -> config.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat(9)));
            default -> throw new AssertionError(docValues);
        }
        if (index.deletes) {
            // Merging purges the deletes.
            config.setMergePolicy(NoMergePolicy.INSTANCE);
        }
        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir, config)) {
            for (int d = 0; d < index.numDocs; d++) {
                Document doc = new Document();
                doc.add(new StringField("kw", d % 3 == 0 ? "a" : "b", StringField.Store.NO));
                doc.add(new StringField("id", Integer.toString(d), StringField.Store.NO));
                values[d] = index.extremeValues ? randomFrom(Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L, 1L, randomLong())
                    : randomBoolean() ? randomLong()
                    : randomLongBetween(-1000, 1000);
                hasValue[d] = index.sparse == false || randomBoolean();
                if (hasValue[d]) {
                    doc.add(new NumericDocValuesField(FIELD, values[d]));
                }
                writer.addDocument(doc);
            }
            if (index.deletes) {
                for (int d = 0; d < index.numDocs; d++) {
                    if (randomInt(9) == 0) {
                        writer.deleteDocuments(new Term("id", Integer.toString(d)));
                        deleted[d] = true;
                    }
                }
            } else if (randomBoolean()) {
                writer.forceMerge(between(1, 3));
            }
            try (IndexReader reader = writer.getReader()) {
                BigInteger expectedSum = BigInteger.ZERO;
                long expectedCount = 0;
                for (int d = 0; d < index.numDocs; d++) {
                    boolean matches = query instanceof TermQuery == false || (d % 3 == 0 && "a".equals(termValue(query)));
                    if (matches && deleted[d] == false && hasValue[d]) {
                        expectedSum = expectedSum.add(BigInteger.valueOf(values[d]));
                        expectedCount++;
                    }
                }
                return runOperator(
                    reader,
                    query,
                    expectedSum,
                    expectedCount,
                    "docValues=" + docValues + " index=" + index.numDocs + " segments=" + reader.leaves().size()
                );
            }
        }
    }

    private static String termValue(Query query) {
        return ((TermQuery) query).getTerm().text();
    }

    private Counts runOperator(IndexReader reader, Query query, BigInteger expectedSum, long expectedCount, String indexDescription) {
        return runOperator(List.of(reader), query, expectedSum, expectedCount, indexDescription);
    }

    /** Runs the operator over the shards, one per reader. */
    private Counts runOperator(
        List<IndexReader> readers,
        Query query,
        BigInteger expectedSum,
        long expectedCount,
        String indexDescription
    ) {
        List<ShardContext> shards = new ArrayList<>();
        for (int shard = 0; shard < readers.size(); shard++) {
            shards.add(new LuceneSourceOperatorTests.MockShardContext(readers.get(shard), shard));
        }
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(query, List.of())
        );
        ValuesSourceReaderOperator.FieldInfo field = new ValuesSourceReaderOperator.FieldInfo(
            FIELD,
            ElementType.LONG,
            false,
            (driverContext, shard) -> ValuesSourceReaderOperator.load(new LongsBlockLoader(FIELD))
        );
        DataPartitioning partitioning = randomFrom(DataPartitioning.SHARD, DataPartitioning.SEGMENT, DataPartitioning.DOC);
        int concurrency = between(1, 4);
        int minDocsPerSlice = randomBoolean() ? LuceneSliceQueue.MIN_DOCS_PER_SLICE : between(50, 2000);
        String description = indexDescription
            + " partitioning="
            + partitioning
            + " concurrency="
            + concurrency
            + " minDocs="
            + minDocsPerSlice;
        LuceneSumCountLongOperator.Factory factory = new LuceneSumCountLongOperator.Factory(
            new IndexedByShardIdFromList<>(shards),
            queryFunction,
            partitioning,
            1,
            concurrency,
            field,
            () -> 0L,
            minDocsPerSlice,
            QueryWarnings.EMIT
        );

        List<Page> results = new CopyOnWriteArrayList<>();
        List<Driver> drivers = new ArrayList<>();
        List<SourceOperator> operators = new ArrayList<>();
        for (int i = 0; i < Math.max(1, factory.taskConcurrency()); i++) {
            DriverContext driverContext = new DriverContext(blockFactory().bigArrays(), blockFactory(), null);
            SourceOperator operator = factory.get(driverContext);
            operators.add(operator);
            drivers.add(TestDriverFactory.create(driverContext, operator, List.of(), new TestResultPageSinkOperator(results::add)));
        }
        new TestDriverRunner().run(drivers);

        BigDecimal sum = BigDecimal.ZERO;
        long count = 0;
        for (Page page : results) {
            assertThat(page.getPositionCount(), equalTo(1));
            assertThat(page.getBlockCount(), equalTo(5));
            double value = page.<DoubleBlock>getBlock(0).getDouble(0);
            double delta = page.<DoubleBlock>getBlock(1).getDouble(0);
            boolean sumSeen = page.<BooleanBlock>getBlock(2).getBoolean(0);
            long pageCount = page.<LongBlock>getBlock(3).getLong(0);
            assertThat(sumSeen, equalTo(pageCount > 0));
            assertTrue(page.<BooleanBlock>getBlock(4).getBoolean(0));
            // value + delta is the exact sum of the page
            sum = sum.add(new BigDecimal(value)).add(new BigDecimal(delta));
            count += pageCount;
            page.releaseBlocks();
        }
        assertThat(results.size(), equalTo(drivers.size()));
        assertThat(description, sum.toBigIntegerExact(), equalTo(expectedSum));
        assertThat(description, count, equalTo(expectedCount));

        long docsWithoutIds = 0;
        long docsWithIds = 0;
        for (SourceOperator operator : operators) {
            docsWithoutIds += ((LuceneSumCountLongOperator) operator).docsWithoutIds();
            docsWithIds += ((LuceneSumCountLongOperator) operator).docsWithIds();
        }
        return new Counts(count, docsWithoutIds, docsWithIds);
    }
}
