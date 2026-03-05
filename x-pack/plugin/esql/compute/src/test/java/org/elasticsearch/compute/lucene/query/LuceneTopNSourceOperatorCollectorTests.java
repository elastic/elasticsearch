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
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopScoreDocCollectorManager;
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
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class LuceneTopNSourceOperatorCollectorTests extends ComputeTestCase {

    private final Directory directory = newDirectory();
    private IndexReader reader;

    @After
    public void closeIndex() throws IOException {
        IOUtils.close(reader, directory);
    }

    public void testTopScoreDocCollectorManagerIsReused() throws IOException {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        var manager1 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, notNullValue());

        var manager2 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, sameInstance(manager2));
        manager1.newCollector();
        manager1.newCollector();
        var manager3 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, sameInstance(manager3));
    }

    public void testTopFieldCollectorManagerReuseForSameSort() throws IOException {
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
        var factory = createFactory(randomBoolean(), sorts, DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        Sort fieldSort = new Sort(new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

        var manager1 = provider.getTopFieldCollectorManager(fieldSort);
        assertThat(manager1, notNullValue());
        var manager2 = provider.getTopFieldCollectorManager(fieldSort);
        assertThat(manager1, sameInstance(manager2));
        manager1.newCollector();
        var manager3 = provider.getTopFieldCollectorManager(fieldSort);
        assertThat(manager1, sameInstance(manager3));
    }

    public void testDifferentSortsGetDifferentManagers() throws IOException {
        var factory = createFactory(randomBoolean(), List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        Sort sort1 = new Sort(SortField.FIELD_SCORE);
        Sort sort2 = new Sort(SortField.FIELD_DOC);

        var manager1 = provider.getTopFieldCollectorManager(sort1);
        var manager2 = provider.getTopFieldCollectorManager(sort2);

        assertThat(manager1, not(sameInstance(manager2)));
        var manager1Again = provider.getTopFieldCollectorManager(sort1);
        assertThat(manager1, sameInstance(manager1Again));
    }

    public void testLazyInitializationOfTopScoreDocCollectorManager() throws IOException {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        assertThat(provider.topScoreDocCollectorManager, nullValue());

        var manager = provider.getTopScoreDocCollectorManager();
        assertThat(provider.topScoreDocCollectorManager, notNullValue());
        assertThat(provider.topScoreDocCollectorManager, sameInstance(manager));
    }

    public void testLazyInitializationOfTopFieldCollectorManager() throws IOException {
        var factory = createFactory(randomBoolean(), List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(true));
        Sort sort = new Sort(SortField.FIELD_DOC);

        var manager = provider.getTopFieldCollectorManager(sort);

        assertThat(provider.topFieldCollectorManagers.size(), equalTo(1));
        assertThat(provider.topFieldCollectorManagers.get(sort), sameInstance(manager));
    }

    public void testConcurrentAccessToTopScoreDocCollectorManager() throws Exception {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        int numThreads = 20;
        var barrier = new CyclicBarrier(numThreads);
        var managers = new ArrayList<TopScoreDocCollectorManager>();
        var errors = new ArrayList<Throwable>();

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await(); // Synchronize all threads to start at the same time
                    var manager = provider.getTopScoreDocCollectorManager();
                    synchronized (managers) {
                        managers.add(manager);
                    }
                } catch (Exception e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Check for errors
        if (errors.isEmpty() == false) {
            fail(errors.getFirst(), "Concurrent access failed");
        }

        // All threads should have gotten the SAME manager instance
        assertThat(managers.size(), equalTo(numThreads));
        TopScoreDocCollectorManager firstManager = managers.getFirst();
        for (TopScoreDocCollectorManager manager : managers) {
            assertThat(manager, sameInstance(firstManager));
        }
    }

    public void testConcurrentAccessToCollectors() throws Exception {
        var factory = createFactory(randomBoolean(), List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        int numThreads = 100;
        var barrier = new CyclicBarrier(numThreads);
        var errors = new ArrayList<Throwable>();

        Sort sort = switch (randomInt(2)) {
            case 0 -> Sort.RELEVANCE;
            case 1 -> new Sort(SortField.FIELD_DOC, SortField.FIELD_SCORE);
            case 2 -> new Sort(new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));
            default -> throw new IllegalStateException("Unexpected value");
        };
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < randomIntBetween(500, 1000); j++) {
                        provider.newTopDocsCollector(sort);
                    }
                } catch (Exception e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Check for errors
        if (errors.isEmpty() == false) {
            fail(errors.getFirst(), "Concurrent access failed");
        }
    }

    public void testConcurrentAccessToTopFieldCollectorManager() throws Exception {
        var factory = createFactory(randomBoolean(), List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        Sort sort = new Sort(SortField.FIELD_DOC);
        int numThreads = 20;
        var barrier = new CyclicBarrier(numThreads);
        var managers = new ArrayList<TopFieldCollectorManager>();
        var errors = new ArrayList<Throwable>();

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                try {
                    barrier.await();
                    var manager = provider.getTopFieldCollectorManager(sort);
                    synchronized (managers) {
                        managers.add(manager);
                    }
                } catch (Exception e) {
                    synchronized (errors) {
                        errors.add(e);
                    }
                }
            });
            threads[i].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Check for errors
        if (errors.isEmpty() == false) {
            fail(errors.getFirst(), "Concurrent access failed");
        }

        // All threads should have gotten the SAME manager instance
        assertThat(managers.size(), equalTo(numThreads));
        TopFieldCollectorManager firstManager = managers.getFirst();
        for (TopFieldCollectorManager manager : managers) {
            assertThat(manager, sameInstance(firstManager));
        }
    }

    public void testDifferentFactoriesHaveDifferentProviders() throws IOException {
        var provider1 = new LuceneTopNSourceOperator.PerShardCollectorProvider(100, true, List.of());
        var provider2 = new LuceneTopNSourceOperator.PerShardCollectorProvider(100, true, List.of());

        assertThat(provider1, not(sameInstance(provider2)));

        var manager1 = provider1.getTopScoreDocCollectorManager();
        var manager2 = provider2.getTopScoreDocCollectorManager();
        assertThat(manager1, not(sameInstance(manager2)));
    }

    public void testProviderWithoutScoreDoesNotCreateTopScoreDocCollectorManagerEagerly() throws IOException {
        var factory = createFactory(randomBoolean(), List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        assertThat(provider.topScoreDocCollectorManager, nullValue());

        var manager = provider.getTopScoreDocCollectorManager();
        assertThat(manager, notNullValue());
        assertThat(provider.topScoreDocCollectorManager, sameInstance(manager));
    }

    public void testNewPerShardCollectorCreatesDifferentCollectorInstances() throws IOException {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        var ctx = createMockShardContext(0);

        var collector1 = provider.newPerShardCollector(ctx);
        var collector2 = provider.newPerShardCollector(ctx);

        assertThat(collector1, not(sameInstance(collector2)));
        assertThat(collector1.collector, not(sameInstance(collector2.collector)));
    }

    public void testNewTopDocsCollectorReusesManagersButCreatesNewCollectors() throws IOException {
        var factory = createFactory(true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        Sort relevanceSort = Sort.RELEVANCE;

        var collector1 = provider.newTopDocsCollector(relevanceSort);
        var collector2 = provider.newTopDocsCollector(relevanceSort);
        var collector3 = provider.newTopDocsCollector(relevanceSort);

        assertThat(collector1, not(sameInstance(collector2)));
        assertThat(collector2, not(sameInstance(collector3)));
        var manager1 = provider.getTopScoreDocCollectorManager();
        var manager2 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, sameInstance(manager2));
    }

    public void testMultipleOperatorsShareProvider() throws Exception {
        var factory = createFactory(randomBoolean(), List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);

        int numOperators = 4;
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

            var sort = new Sort(new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));
            var manager1 = sharedProvider.getTopFieldCollectorManager(sort);
            var manager2 = sharedProvider.getTopFieldCollectorManager(sort);
            assertThat(manager1, sameInstance(manager2));
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
        return new LuceneSourceOperatorTests.MockShardContext(reader, shardId) {
            @Override
            public Optional<SortAndFormats> buildSort(List<SortBuilder<?>> sorts) {
                // Always return a simple field sort for testing purposes
                // This matches the pattern from LuceneTopNSourceOperatorTests
                SortField field = new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN);
                return Optional.of(new SortAndFormats(new Sort(field), new DocValueFormat[] { DocValueFormat.RAW }));
            }
        };
    }

    private LuceneTopNSourceOperator.Factory createFactory(boolean needsScore, List<SortBuilder<?>> sorts, DataPartitioning partitioning)
        throws IOException {
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
        );

        return new LuceneTopNSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            partitioning,
            randomIntBetween(1, 10),
            10000,
            randomIntBetween(10, 100),
            sorts,
            randomIntBetween(10, 20),
            needsScore
        );
    }

    private DriverContext createDriverContext() {
        var blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
