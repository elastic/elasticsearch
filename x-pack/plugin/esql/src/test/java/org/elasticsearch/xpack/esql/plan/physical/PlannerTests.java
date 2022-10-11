/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.compute.transport.ComputeRequest;
import org.elasticsearch.xpack.esql.plan.physical.old.OldLocalExecutionPlanner;
import org.elasticsearch.xpack.esql.plan.physical.old.OldLocalExecutionPlanner.IndexReaderReference;
import org.elasticsearch.xpack.esql.plan.physical.old.PlanNode;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class PlannerTests extends ESTestCase {

    private ThreadPool threadPool;
    Directory dir;
    IndexReader indexReader;

    int numDocs = 1000000;

    int numGroups = randomIntBetween(1, 10);

    int maxNumSegments = randomIntBetween(1, 100);

    private final int defaultTaskConcurrency = ThreadPool.searchThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY));

    int segmentLevelConcurrency = 0;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path path = createTempDir();
        dir = new MMapDirectory(path);
        logger.info("indexing started");
        try (IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            NumericDocValuesField docValuesGroupField = new NumericDocValuesField("id", 0);
            NumericDocValuesField docValuesField = new NumericDocValuesField("value", 0);
            for (int i = 0; i < numDocs; i++) {
                doc.clear();
                docValuesGroupField.setLongValue(i % numGroups);
                docValuesField.setLongValue(i);
                doc.add(docValuesGroupField);
                doc.add(docValuesField);
                indexWriter.addDocument(doc);
                if (i % 10000 == 9999) {
                    indexWriter.flush();
                }
            }
            indexWriter.forceMerge(maxNumSegments);
            indexWriter.flush();
            indexWriter.commit();
        }
        logger.info("indexing completed");
        indexReader = DirectoryReader.open(dir);
        segmentLevelConcurrency = LuceneSourceOperator.numSegmentSlices(indexReader);
        threadPool = new TestThreadPool("PlannerTests");
    }

    double expectedGroupAvg(int groupId) {
        long total = 0;
        long count = 0;
        for (int i = groupId; i < numDocs; i += numGroups) {
            total += i;
            count++;
        }
        return (double) total / count;
    }

    @After
    public void tearDown() throws Exception {
        indexReader.close();
        dir.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    private void runAndCheckNoGrouping(PlanNode.Builder planNodeBuilder, int... expectedDriverCounts) {
        BiConsumer<List<String>, Page> pageAsserter = (columns, page) -> {
            logger.info("New page: columns {}, values {}", columns, page);
            assertEquals(Arrays.asList("value_avg"), columns);
            assertEquals(1, page.getPositionCount());
            assertEquals(((double) numDocs - 1) / 2, page.getBlock(0).getDouble(0), 0.1d);
        };
        runAndCheck(planNodeBuilder, pageAsserter, expectedDriverCounts);
    }

    private void runAndCheckWithGrouping(PlanNode.Builder planNodeBuilder, int... expectedDriverCounts) {
        BiConsumer<List<String>, Page> pageAsserter = (columns, page) -> {
            logger.info("New page: columns {}, values {}", columns, page);
            assertEquals(List.of("id", "value_avg"), columns);
            assertEquals(numGroups, page.getPositionCount());
            Block groupIdBlock = page.getBlock(0);
            for (int i = 0; i < numGroups; i++) {
                assertEquals(expectedGroupAvg((int) groupIdBlock.getLong(i)), page.getBlock(1).getDouble(i), 0.1d);
            }
        };
        runAndCheck(planNodeBuilder, pageAsserter, expectedDriverCounts);
    }

    private void runAndCheck(PlanNode.Builder planNodeBuilder, BiConsumer<List<String>, Page> pageAsserter, int... expectedDriverCounts) {
        PlanNode plan = planNodeBuilder.build(pageAsserter);
        logger.info("Plan: {}", Strings.toString(new ComputeRequest(planNodeBuilder.buildWithoutOutputNode()), true, true));
        try (
            XContentParser parser = createParser(
                parserConfig().withRegistry(new NamedXContentRegistry(PlanNode.getNamedXContentParsers())),
                JsonXContent.jsonXContent,
                new BytesArray(
                    Strings.toString(new ComputeRequest(planNodeBuilder.buildWithoutOutputNode()), true, true)
                        .getBytes(StandardCharsets.UTF_8)
                )
            )
        ) {
            ComputeRequest.fromXContent(parser);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        OldLocalExecutionPlanner.LocalExecutionPlan localExecutionPlan = new OldLocalExecutionPlanner(
            List.of(new IndexReaderReference(indexReader, new ShardId("test", "test", 0)))
        ).plan(plan);
        assertArrayEquals(
            expectedDriverCounts,
            localExecutionPlan.getDriverFactories().stream().mapToInt(OldLocalExecutionPlanner.DriverFactory::driverInstances).toArray()
        );
        Driver.runToCompletion(threadPool.executor(ThreadPool.Names.SEARCH), localExecutionPlan.createDrivers());
    }

    public void testAvgSingleThreaded() {
        runAndCheckNoGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE, "test")
                .numericDocValues("value")
                .avg("value"),
            1
        );
    }

    public void testAvgGroupingSingleThreaded() {
        runAndCheckWithGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE, "test")
                .numericDocValues("id")
                .numericDocValues("value")
                .avgGrouping("id", "value"),
            1
        );
    }

    public void testAvgWithSegmentLevelParallelism() {
        runAndCheckNoGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT, "test")
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            segmentLevelConcurrency,
            1
        );
    }

    public void testAvgGroupingWithSegmentLevelParallelism() {
        runAndCheckWithGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT, "test")
                .numericDocValues("id")
                .numericDocValues("value")
                .avgGroupingPartial("id", "value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgGroupingFinal("id", "value"),
            segmentLevelConcurrency,
            1
        );
    }

    public void testAvgWithDocLevelParallelism() {
        runAndCheckNoGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.DOC, "test")
                .numericDocValues("value")
                .avgPartial("value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgFinal("value"),
            defaultTaskConcurrency,
            1
        );
    }

    public void testAvgWithSingleThreadedSearchButParallelAvg() {
        runAndCheckNoGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE, "test")
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

    public void testAvgGroupingWithSingleThreadedSearchButParallelAvg() {
        runAndCheckWithGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SINGLE, "test")
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("value")
                .numericDocValues("id")
                .avgGroupingPartial("id", "value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgGroupingFinal("id", "value"),
            1,
            defaultTaskConcurrency,
            1
        );
    }

    public void testAvgWithSegmentLevelParallelismAndExtraParallelAvg() {
        runAndCheckNoGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT, "test")
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

    public void testAvgGroupingWithSegmentLevelParallelismAndExtraParallelAvg() {
        runAndCheckWithGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.SEGMENT, "test")
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("id")
                .numericDocValues("value")
                .avgGroupingPartial("id", "value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgGroupingFinal("id", "value"),
            segmentLevelConcurrency,
            defaultTaskConcurrency,
            1
        );
    }

    public void testAvgWithDocLevelParallelismAndExtraParallelAvg() {
        runAndCheckNoGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.DOC, "test")
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

    public void testAvgGroupingWithDocLevelParallelismAndExtraParallelAvg() {
        runAndCheckWithGrouping(
            PlanNode.builder(new MatchAllDocsQuery(), PlanNode.LuceneSourceNode.Parallelism.DOC, "test")
                .exchange(PlanNode.ExchangeNode.Type.REPARTITION, PlanNode.ExchangeNode.Partitioning.FIXED_ARBITRARY_DISTRIBUTION)
                .numericDocValues("id")
                .numericDocValues("value")
                .avgGroupingPartial("id", "value")
                .exchange(PlanNode.ExchangeNode.Type.GATHER, PlanNode.ExchangeNode.Partitioning.SINGLE_DISTRIBUTION)
                .avgGroupingFinal("id", "value"),
            defaultTaskConcurrency,
            defaultTaskConcurrency,
            1
        );
    }
}
