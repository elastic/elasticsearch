/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.EsNumericRangeQuery;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that {@link LuceneSourceOperator} correctly uses
 * {@link BlockLoader.OptionalBulkNumericFilter} when available on the field's doc values.
 */
public class LuceneSourceOperatorBulkFilterTests extends ComputeTestCase {

    private Directory directory;
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        if (reader != null) reader.close();
        if (directory != null) directory.close();
    }

    public void testBulkFilterEmitsOnlyMatchingDocs() throws IOException {
        int numDocs = randomIntBetween(200, 600);
        long lower = randomIntBetween(10, numDocs / 3);
        long upper = randomIntBetween((int) lower, numDocs - 1);
        runTest(numDocs, lower, upper, LuceneOperator.NO_LIMIT);
    }

    public void testBulkFilterWithLimit() throws IOException {
        int numDocs = randomIntBetween(200, 600);
        long lower = 0;
        long upper = numDocs - 1;
        int limit = randomIntBetween(1, numDocs / 2);
        runTest(numDocs, lower, upper, limit);
    }

    public void testBulkFilterEmptyResult() throws IOException {
        int numDocs = randomIntBetween(100, 300);
        // filter range outside all values
        runTest(numDocs, numDocs + 1, numDocs + 100, LuceneOperator.NO_LIMIT);
    }

    /**
     * Runs the operator with a bulk filter on [lower, upper] and verifies results.
     * Each doc has value == its segment-local docId, making expected output easy to compute.
     */
    private void runTest(int numDocs, long lower, long upper, int limit) throws IOException {
        directory = newDirectory();
        // Force single segment so segment-local docId == insertion order
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int d = 0; d < numDocs; d++) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("value", d));
                writer.addDocument(doc);
            }
            writer.commit();
        }

        DirectoryReader baseReader = DirectoryReader.open(directory);
        // Wrap to inject our OptionalBulkNumericFilter implementation
        reader = new BulkFilterDirectoryReader(baseReader, numDocs, "value");

        ShardContext shardContext = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        int maxPageSize = randomIntBetween(10, Math.max(10, numDocs / 3));

        LuceneSourceOperator.Factory factory = new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(shardContext),
            c -> List.of(
                new LuceneSliceQueue.QueryAndTags(new EsNumericRangeQuery(Queries.ALL_DOCS_INSTANCE, "value", lower, upper), List.of())
            ),
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            1,
            maxPageSize,
            limit,
            false
        );

        DriverContext driverContext = driverContext();
        List<Page> results = new ArrayList<>();
        new TestDriverRunner().run(
            TestDriverFactory.create(driverContext, factory.get(driverContext), List.of(), new TestResultPageSinkOperator(results::add))
        );

        // Collect all emitted doc IDs across pages
        TreeSet<Integer> emittedDocs = new TreeSet<>();
        for (Page page : results) {
            DocVector docVector = ((DocBlock) page.getBlock(0)).asVector();
            for (int i = 0; i < docVector.getPositionCount(); i++) {
                emittedDocs.add(docVector.docs().getInt(i));
            }
            page.releaseBlocks();
        }

        // Build expected set: docs with value in [lower, upper], capped by limit
        TreeSet<Integer> expected = new TreeSet<>();
        for (int d = 0; d < numDocs; d++) {
            if (d >= lower && d <= upper) {
                expected.add(d);
                if (limit != LuceneOperator.NO_LIMIT && expected.size() >= limit) {
                    break;
                }
            }
        }

        assertThat("emitted doc IDs mismatch for range [" + lower + ", " + upper + "]", emittedDocs, equalTo(expected));
    }

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    /**
     * {@link NumericDocValues} that also implements {@link BlockLoader.OptionalBulkNumericFilter}.
     * Values are {@code values[docId]}.
     */
    private static class BulkFilterableNumericDocValues extends NumericDocValues implements BlockLoader.OptionalBulkNumericFilter {
        private final long[] values;
        private int docId = -1;

        BulkFilterableNumericDocValues(long[] values) {
            this.values = values;
        }

        @Override
        public long longValue() {
            return values[docId];
        }

        @Override
        public boolean advanceExact(int target) {
            docId = target;
            return target < values.length;
        }

        @Override
        public int docID() {
            return docId;
        }

        @Override
        public int nextDoc() {
            return advance(docId + 1);
        }

        @Override
        public int advance(int target) {
            docId = target >= values.length ? NO_MORE_DOCS : target;
            return docId;
        }

        @Override
        public long cost() {
            return values.length;
        }

        @Override
        public boolean tryBulkRangeFilter(int startDoc, long lower, long upper, boolean[] mask) {
            for (int i = 0; i < mask.length; i++) {
                mask[i] = values[startDoc + i] >= lower && values[startDoc + i] <= upper;
            }
            return true;
        }
    }

    private static class BulkFilterLeafReader extends FilterLeafReader {
        private final long[] values;
        private final String fieldName;

        BulkFilterLeafReader(LeafReader in, long[] values, String fieldName) {
            super(in);
            this.values = values;
            this.fieldName = fieldName;
        }

        @Override
        public NumericDocValues getNumericDocValues(String field) throws IOException {
            if (fieldName.equals(field)) {
                return new BulkFilterableNumericDocValues(values);
            }
            return super.getNumericDocValues(field);
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

    private static class BulkFilterDirectoryReader extends FilterDirectoryReader {
        private final long[] values;
        private final String fieldName;

        BulkFilterDirectoryReader(DirectoryReader in, int numDocs, String fieldName) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    long[] vals = new long[numDocs];
                    for (int i = 0; i < numDocs; i++) {
                        vals[i] = i;
                    }
                    return new BulkFilterLeafReader(reader, vals, fieldName);
                }
            });
            long[] vals = new long[numDocs];
            for (int i = 0; i < numDocs; i++) {
                vals[i] = i;
            }
            this.values = vals;
            this.fieldName = fieldName;
        }

        private BulkFilterDirectoryReader(DirectoryReader in, long[] values, String fieldName) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new BulkFilterLeafReader(reader, values, fieldName);
                }
            });
            this.values = values;
            this.fieldName = fieldName;
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new BulkFilterDirectoryReader(in, values, fieldName);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }
}
