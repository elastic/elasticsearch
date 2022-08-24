/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.sql.action.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.Driver;
import org.elasticsearch.xpack.sql.action.compute.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.sql.action.compute.planner.LocalExecutionPlanner.IndexReaderReference;
import org.elasticsearch.xpack.sql.action.compute.planner.PlanNode;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.sql.action.compute.planner.LocalExecutionPlanner.DEFAULT_TASK_CONCURRENCY;

public class MultiShardPlannerTests extends ESTestCase {
    private ThreadPool threadPool;
    List<Directory> dirs = new ArrayList<>();
    List<IndexReaderReference> indexReaders = new ArrayList<>();

    int numDocs = 1000000;

    int maxNumSegments = randomIntBetween(1, 100);

    int segmentLevelConcurrency = 0;
    int shardCount = 2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path path = createTempDir();
        for (int shardId = 0; shardId < shardCount; shardId++) {
            Directory dir = new MMapDirectory(path);
            dirs.add(dir);
            logger.info("indexing started");
            try (IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                NumericDocValuesField docValuesField = new NumericDocValuesField("value", 0);
                for (int i = 0; i < numDocs; i++) {
                    doc.clear();
                    docValuesField.setLongValue(i);
                    doc.add(docValuesField);
                    indexWriter.addDocument(doc);
                }
                indexWriter.commit();
                indexWriter.forceMerge(maxNumSegments);
                indexWriter.flush();
            }
            logger.info("indexing completed");
            IndexReader indexReader = DirectoryReader.open(dir);
            indexReaders.add(new IndexReaderReference(indexReader, new ShardId("test", "test", shardId)));
            segmentLevelConcurrency += LuceneSourceOperator.numSegmentSlices(indexReader);
        }
        threadPool = new TestThreadPool("PlannerTests");
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.close(indexReaders.stream().map(IndexReaderReference::indexReader).collect(Collectors.toList()));
        IOUtils.close(dirs);
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    private void runAndCheck(PlanNode.Builder planNodeBuilder, int... expectedDriverCounts) {
        PlanNode plan = planNodeBuilder.build((columns, page) -> {
            logger.info("New page: columns {}, values {}", columns, page);
            assertEquals(Arrays.asList("value_avg"), columns);
            assertEquals(1, page.getPositionCount());
            assertEquals((numDocs - 1) / 2, page.getBlock(0).getLong(0));
        });
        logger.info("Plan: {}", Strings.toString(plan, true, true));
        LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = new LocalExecutionPlanner(indexReaders).plan(plan);
        assertArrayEquals(
            expectedDriverCounts,
            localExecutionPlan.getDriverFactories().stream().mapToInt(LocalExecutionPlanner.DriverFactory::driverInstances).toArray()
        );
        Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), localExecutionPlan.createDrivers());
    }

    public void testAvgSingleThreaded() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE, "test")
                .numericDocValues("value")
                .avg("value"),
            shardCount
        );
    }

    public void testAvgWithSegmentLevelParallelism() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT, "test")
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            segmentLevelConcurrency,
            1
        );
    }

    public void testAvgWithDocLevelParallelism() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.DOC, "test")
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            DEFAULT_TASK_CONCURRENCY * shardCount,
            1
        );
    }

    public void testAvgWithSingleThreadedSearchButParallelAvg() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE, "test")
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            shardCount,
            DEFAULT_TASK_CONCURRENCY,
            1
        );
    }

    public void testAvgWithSegmentLevelParallelismAndExtraParallelAvg() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT, "test")
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            segmentLevelConcurrency,
            DEFAULT_TASK_CONCURRENCY,
            1
        );
    }

    public void testAvgWithDocLevelParallelismAndExtraParallelAvg() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.DOC, "test")
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            DEFAULT_TASK_CONCURRENCY * shardCount,
            DEFAULT_TASK_CONCURRENCY,
            1
        );
    }
}
