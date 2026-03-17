/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.ShardContext;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortMode;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LuceneTopNSourceOperatorCollectorTests extends ComputeTestCase {

    private final Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    public void testRelevanceSortCreatesTopScoreDocCollectorManager() throws IOException {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // TopScoreDocCollectorManager should be created during construction when needsScore=true and sorts are empty
        assertThat(provider.topScoreDocCollectorManager, notNullValue());

        // Get a collector from the provider - should work without errors
        var ctx = createMockShardContext(0, true);
        var perShardCollector = provider.newPerShardCollector(ctx);
        assertThat(perShardCollector.collector, notNullValue());
    }

    public void testFieldSortCreatesTopFieldCollectorManager() throws IOException {
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
        var factory = createFactory(false, sorts, DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // TopFieldCollectorManagers should be created during construction
        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(false));

        // Get a collector from the provider - should work without errors
        var ctx = createMockShardContext(0);
        var perShardCollector = provider.newPerShardCollector(ctx);
        assertThat(perShardCollector.collector, notNullValue());
    }

    public void testFieldSortWithScoreCreatesTopFieldCollectorManager() throws IOException {
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
        var factory = createFactory(true, sorts, DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // TopFieldCollectorManagers should be created during construction
        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(false));

        // Get a collector from the provider - should work without errors
        var ctx = createMockShardContext(0);
        var perShardCollector = provider.newPerShardCollector(ctx);
        assertThat(perShardCollector.collector, notNullValue());
    }

    public void testNewPerShardCollectorCreatesNewInstancesEachTime() throws IOException {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        // Use relevance sort mock to match factory creation
        var ctx = createMockShardContext(0, true);

        var perShardCollector1 = provider.newPerShardCollector(ctx);
        var perShardCollector2 = provider.newPerShardCollector(ctx);
        var perShardCollector3 = provider.newPerShardCollector(ctx);

        assertThat(perShardCollector1.collector, not(sameInstance(perShardCollector2.collector)));
        assertThat(perShardCollector2.collector, not(sameInstance(perShardCollector3.collector)));
    }

    public void testNewPerShardCollectorCreatesDifferentCollectorInstances() throws IOException {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        // Use relevance sort mock to match factory creation
        var ctx = createMockShardContext(0, true);

        var collector1 = provider.newPerShardCollector(ctx);
        var collector2 = provider.newPerShardCollector(ctx);

        assertThat(collector1, not(sameInstance(collector2)));
        assertThat(collector1.collector, not(sameInstance(collector2.collector)));
    }

    public void testMultipleOperatorsShareProviderWithJustScore() throws Exception {
        var factory = createFactory(true, List.of(SortBuilders.scoreSort()), DataPartitioning.SHARD);

        int numOperators = randomIntBetween(4, 10);
        var operators = new ArrayList<LuceneTopNSourceOperator>();

        try {
            for (int i = 0; i < numOperators; i++) {
                operators.add((LuceneTopNSourceOperator) factory.get(createDriverContext()));
            }

            var sharedProvider = operators.get(0).perShardCollectorProvider;
            for (var op : operators) {
                assertThat(op.perShardCollectorProvider, sameInstance(sharedProvider));
            }

            assertThat(sharedProvider, notNullValue());

            var manager1 = sharedProvider.getTopScoreDocCollectorManager();
            var manager2 = sharedProvider.getTopScoreDocCollectorManager();
            assertThat(manager1, sameInstance(manager2));

            // Concurrent section: multiple threads call newPerShardCollector simultaneously
            var ctx = createMockShardContext(0, true);
            int numThreads = randomIntBetween(4, 8);
            int callsPerThread = randomIntBetween(5, 15);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            var allCollectors = new CopyOnWriteArrayList<>();

            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < numThreads; t++) {
                    futures.add(executor.submit(() -> {
                        barrier.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            allCollectors.add(sharedProvider.newPerShardCollector(ctx).collector);
                        }
                        return null;
                    }));
                }
                for (var f : futures) f.get();
            } finally {
                executor.shutdownNow();
            }

            Set<Object> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
            distinct.addAll(allCollectors);
            assertThat(distinct.size(), equalTo(numThreads * callsPerThread));
        } finally {
            IOUtils.close(operators);
        }
    }

    public void testMultipleOperatorsShareProviderWithScoreNotNeeded() throws Exception {
        testMultipleOperatorsShareProviderWithoutScore(true);
    }

    public void testMultipleOperatorsShareProviderWithScoreNeeded() throws Exception {
        testMultipleOperatorsShareProviderWithoutScore(false);
    }

    private void testMultipleOperatorsShareProviderWithoutScore(boolean needsScore) throws Exception {
        Sort sort = new Sort(new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));
        var factory = createFactory(
            needsScore,
            List.of(SortBuilders.fieldSort("s").order(SortOrder.ASC).sortMode(SortMode.MIN)),
            DataPartitioning.SHARD
        );

        int numOperators = randomIntBetween(4, 10);
        var operators = new ArrayList<LuceneTopNSourceOperator>();

        try {
            for (int i = 0; i < numOperators; i++) {
                operators.add((LuceneTopNSourceOperator) factory.get(createDriverContext()));
            }

            var sharedProvider = operators.get(0).perShardCollectorProvider;
            for (var op : operators) {
                assertThat(op.perShardCollectorProvider, sameInstance(sharedProvider));
            }

            assertThat(sharedProvider, notNullValue());

            var manager1 = sharedProvider.getTopDocsCollectorForSort(sort);
            var manager2 = sharedProvider.getTopDocsCollectorForSort(sort);
            assertNotSame(manager1, manager2);

            // Concurrent section: multiple threads call getTopDocsCollectorForSort simultaneously
            int numThreads = randomIntBetween(4, 8);
            int callsPerThread = randomIntBetween(5, 15);
            CyclicBarrier barrier = new CyclicBarrier(numThreads);
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            var allCollectors = new CopyOnWriteArrayList<>();

            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < numThreads; t++) {
                    futures.add(executor.submit(() -> {
                        barrier.await();
                        for (int i = 0; i < callsPerThread; i++) {
                            allCollectors.add(sharedProvider.getTopDocsCollectorForSort(sort));
                        }
                        return null;
                    }));
                }
                for (var f : futures) f.get();
            } finally {
                executor.shutdownNow();
            }

            Set<Object> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
            distinct.addAll(allCollectors);
            assertThat(distinct.size(), equalTo(numThreads * callsPerThread));
        } finally {
            IOUtils.close(operators);
        }
    }

    private void setupIndex(int numDocs) throws IOException {
        try (RandomIndexWriter writer = new RandomIndexWriter(random(), directory)) {
            for (int d = 0; d < numDocs; d++) {
                List<IndexableField> doc = new ArrayList<>();
                doc.add(new SortedNumericDocValuesField("s", d));
                writer.addDocument(doc);
            }
            reader = writer.getReader();
        }
    }

    private ShardContext createMockShardContext(int shardId) {
        return createMockShardContext(shardId, false);
    }

    private ShardContext createMockShardContext(int shardId, boolean returnRelevanceSort) {
        return new LuceneSourceOperatorTests.MockShardContext(reader, shardId) {
            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                if (returnRelevanceSort && sorts.isEmpty()) {
                    // Relevance sort has one sort field (FIELD_SCORE)
                    return Optional.of(new SortAndFormats(Sort.RELEVANCE, new DocValueFormat[] { DocValueFormat.RAW }));
                }
                // Return a simple field sort for testing purposes
                SortField field = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
                return Optional.of(new SortAndFormats(new Sort(field), new DocValueFormat[] { DocValueFormat.RAW }));
            }
        };
    }

    private LuceneTopNSourceOperator.Factory createFactory(boolean needsScore, List<SortBuilder<?>> sorts, DataPartitioning partitioning)
        throws IOException {
        setupIndex(100);
        // Use relevance sort mock when needsScore=true and sorts are empty
        boolean useRelevanceSort = needsScore && sorts.isEmpty();
        ShardContext ctx = createMockShardContext(0, useRelevanceSort);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
        );

        return new LuceneTopNSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            partitioning,
            DataPartitioning.AutoStrategy.DEFAULT,
            randomIntBetween(1, 10),
            10000,
            randomIntBetween(10, 100),
            sorts,
            randomLongBetween(10, 20),
            needsScore
        );
    }

    private DriverContext createDriverContext() {
        var blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
