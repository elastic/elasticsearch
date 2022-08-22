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
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.sql.action.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.Driver;
import org.elasticsearch.xpack.sql.action.compute.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.sql.action.compute.planner.PlanNode;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@LuceneTestCase.SuppressCodecs("*")
public class PlannerTests extends ESTestCase {

    private ThreadPool threadPool;
    Directory dir;
    IndexReader indexReader;

    int numDocs = 1000000;

    int maxNumSegments = randomIntBetween(1, 100);

    private final int defaultTaskConcurrency = ThreadPool.searchThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY));

    int segmentLevelConcurrency = 0;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path path = Files.createTempDirectory("test");
        dir = new MMapDirectory(path);
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
        indexReader = DirectoryReader.open(dir);
        segmentLevelConcurrency = LuceneSourceOperator.numSegmentSlices(indexReader);
        threadPool = new TestThreadPool("PlannerTests");
    }

    @After
    public void tearDown() throws Exception {
        indexReader.close();
        dir.close();
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
        LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = new LocalExecutionPlanner(indexReader).plan(plan);
        assertArrayEquals(
            expectedDriverCounts,
            localExecutionPlan.getDriverFactories().stream().mapToInt(LocalExecutionPlanner.DriverFactory::driverInstances).toArray()
        );
        Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), localExecutionPlan.createDrivers());
    }

    public void testAvgSingleThreaded() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE).numericDocValues("value").avg("value"),
            1
        );
    }

    public void testAvgWithSegmentLevelParallelism() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT)
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
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.DOC)
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            defaultTaskConcurrency,
            1
        );
    }

    public void testAvgWithSingleThreadedSearchButParallelAvg() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE)
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            1,
            defaultTaskConcurrency,
            1
        );
    }

    public void testAvgWithSegmentLevelParallelismAndExtraParallelAvg() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT)
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            segmentLevelConcurrency,
            defaultTaskConcurrency,
            1
        );
    }

    public void testAvgWithDocLevelParallelismAndExtraParallelAvg() {
        runAndCheck(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.DOC)
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            defaultTaskConcurrency,
            defaultTaskConcurrency,
            1
        );
    }
}
