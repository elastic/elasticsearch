/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.lucene.query.LuceneOperator;
import org.elasticsearch.compute.lucene.query.LuceneSliceQueue;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperator;
import org.elasticsearch.compute.lucene.query.LuceneSourceOperatorTests;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperator;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorStatus;
import org.elasticsearch.compute.lucene.read.ValuesSourceReaderOperatorTests;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverStatus;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.compute.test.TestDriverFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.compute.test.TestResultPageSinkOperator;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class LuceneDirectoryBytesReadOperatorTests extends ComputeTestCase {

    private static final long COUNT_STEP = 31;
    /** Keep page size small relative to doc count so {@link LuceneSourceOperator} emits multiple pages. */
    private static final int MAX_PAGE_SIZE = 10;
    private static final MappedFieldType LONG_FIELD = new NumberFieldMapper.NumberFieldType("v", NumberFieldMapper.NumberType.LONG);

    private Directory directory;
    private DirectoryReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
        reader = null;
        directory = null;
    }

    public void testLuceneSourceOperatorByteCount() throws IOException {
        runLuceneSourceDriver((status, driver) -> {
            assertBytesReadIncrementsByStep(status.bytesRead());
            assertThat(status.bytesRead(), greaterThanOrEqualTo(COUNT_STEP * status.pagesEmitted()));
        });
    }

    public void testDriverCompletionInfoAddsPlanningBytesOnce() throws IOException {
        runLuceneSourceDriver((status, driver) -> {
            long operatorBytes = status.bytesRead();
            assertThat(operatorBytes, greaterThan(0L));

            long planningBytes = 12_345L;
            DriverCompletionInfo withPlanning = DriverCompletionInfo.excludingProfiles(List.of(driver), planningBytes);
            DriverCompletionInfo withoutPlanning = DriverCompletionInfo.excludingProfiles(List.of(driver), 0L);

            assertThat(withPlanning.bytesRead(), equalTo(planningBytes + operatorBytes));
            assertThat(withoutPlanning.bytesRead(), equalTo(operatorBytes));
            assertThat(withPlanning.bytesRead() - withoutPlanning.bytesRead(), equalTo(planningBytes));
        });
    }

    public void testValuesSourceReaderOperatorAccumulatesPerPageDeltas() throws IOException {
        int numDocs = between(50, 100);
        initIndex(numDocs);
        var directoryBytesRead = new CountingBytesReadProvider();
        DriverContext driverContext = driverContext();
        LuceneSourceOperator.Factory luceneFactory = luceneFactory(directoryBytesRead, reader);
        ValuesSourceReaderOperator.Factory readerFactory = valuesReaderFactory(directoryBytesRead, reader);
        List<Page> pages = new ArrayList<>();
        try (
            Driver driver = TestDriverFactory.create(
                driverContext,
                luceneFactory.get(driverContext),
                List.of(readerFactory.get(driverContext)),
                new TestResultPageSinkOperator(pages::add)
            )
        ) {
            new TestDriverRunner().run(driver);
            assertThat(pages.size(), greaterThan(1));
            ValuesSourceReaderOperatorStatus readerStatus = (ValuesSourceReaderOperatorStatus) driver.profile().operators().get(1).status();
            assertThat(readerStatus.pagesEmitted(), equalTo(pages.size()));
            assertBytesReadIncrementsByStep(readerStatus.bytesRead());
        }
    }

    @FunctionalInterface
    private interface SourceDriverVerifier {
        void verify(LuceneSourceOperator.Status status, Driver driver) throws IOException;
    }

    private void runLuceneSourceDriver(SourceDriverVerifier verifier) throws IOException {
        int numDocs = between(50, 100);
        initIndex(numDocs);
        var directoryBytesRead = new CountingBytesReadProvider();
        DriverContext driverContext = driverContext();
        LuceneSourceOperator.Factory factory = luceneFactory(directoryBytesRead, reader);
        SourceOperator source = factory.get(driverContext);
        List<Page> pages = new ArrayList<>();
        try (Driver driver = TestDriverFactory.create(driverContext, source, List.of(), new TestResultPageSinkOperator(pages::add))) {
            new TestDriverRunner().run(driver);
            assertThat(driver.status().status(), equalTo(DriverStatus.Status.DONE));
            var status = (LuceneSourceOperator.Status) source.status();
            assertThat(pages.size(), greaterThan(1));
            assertThat(status.pagesEmitted(), equalTo(pages.size()));
            verifier.verify(status, driver);
        }
    }

    private static void assertBytesReadIncrementsByStep(long bytesRead) {
        assertThat(bytesRead, greaterThan(0L));
        assertThat(bytesRead % COUNT_STEP, equalTo(0L));
    }

    private void initIndex(int numDocs) throws IOException {
        directory = newDirectory();
        try (
            RandomIndexWriter writer = new RandomIndexWriter(
                ESTestCase.random(),
                directory,
                newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
            )
        ) {
            for (int d = 0; d < numDocs; d++) {
                List<IndexableField> doc = List.of(new LongPoint("v", d));
                writer.addDocument(doc);
            }
            writer.commit();
            reader = writer.getReader();
        }
    }

    private static LuceneSourceOperator.Factory luceneFactory(LongSupplier directoryBytesRead, IndexReader reader) {
        ShardContext ctx = new LuceneSourceOperatorTests.MockShardContext(reader, 0);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
        );
        return new LuceneSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            DataPartitioning.SHARD,
            DataPartitioning.AutoStrategy.DEFAULT,
            LuceneOperator.SMALL_INDEX_BOUNDARY,
            1,
            MAX_PAGE_SIZE,
            LuceneOperator.NO_LIMIT,
            false,
            directoryBytesRead,
            LuceneSliceQueue.MIN_DOCS_PER_SLICE,
            QueryWarnings.EMIT
        );
    }

    private static ValuesSourceReaderOperator.Factory valuesReaderFactory(LongSupplier directoryBytesRead, IndexReader reader) {
        BlockLoader loader = LONG_FIELD.blockLoader(ValuesSourceReaderOperatorTests.blContext());
        return new ValuesSourceReaderOperator.Factory(
            ByteSizeValue.ofGb(1),
            List.of(new ValuesSourceReaderOperator.FieldInfo("v", ElementType.LONG, false, (ctx, shardIdx) -> {
                if (shardIdx != 0) {
                    ESTestCase.fail("unexpected shardIdx [" + shardIdx + "]");
                }
                return ValuesSourceReaderOperator.load(loader);
            })),
            new IndexedByShardIdFromSingleton<>(
                new ValuesSourceReaderOperator.ShardContext(reader, (sourcePaths) -> SourceLoader.FROM_STORED_SOURCE, 0.2)
            ),
            true,
            0,
            1.0,
            500,
            directoryBytesRead
        );
    }

    private DriverContext driverContext() {
        BlockFactory blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }

    /** Monotonic fake store bytes counter. */
    private record CountingBytesReadProvider(AtomicLong counter) implements LongSupplier {
        CountingBytesReadProvider() {
            this(new AtomicLong());
        }

        @Override
        public long getAsLong() {
            return counter.addAndGet(COUNT_STEP);
        }
    }
}
