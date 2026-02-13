/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

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
        var factory = createFactory(100, true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // First call should create the manager
        var manager1 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, notNullValue());

        // Subsequent calls should return the SAME instance
        var manager2 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, sameInstance(manager2));

        // Even after creating multiple collectors from it
        manager1.newCollector();
        manager1.newCollector();
        var manager3 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, sameInstance(manager3));
    }

    public void testTopFieldCollectorManagerReuseForSameSort() throws IOException {
        List<SortBuilder<?>> sorts = List.of(new FieldSortBuilder("s"));
        var factory = createFactory(100, false, sorts, DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        Sort fieldSort = new Sort(new SortedNumericSortField("s", SortField.Type.LONG, false, SortedNumericSelector.Type.MIN));

        // First call creates manager
        var manager1 = provider.getTopFieldCollectorManager(fieldSort);
        assertThat(manager1, notNullValue());

        // Same Sort returns same manager
        var manager2 = provider.getTopFieldCollectorManager(fieldSort);
        assertThat(manager1, sameInstance(manager2));

        // Even after creating collectors
        manager1.newCollector();
        var manager3 = provider.getTopFieldCollectorManager(fieldSort);
        assertThat(manager1, sameInstance(manager3));
    }

    public void testDifferentSortsGetDifferentManagers() throws IOException {
        var factory = createFactory(100, false, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        Sort sort1 = new Sort(SortField.FIELD_SCORE);
        Sort sort2 = new Sort(SortField.FIELD_DOC);

        var manager1 = provider.getTopFieldCollectorManager(sort1);
        var manager2 = provider.getTopFieldCollectorManager(sort2);

        // Different sorts should get different managers
        assertThat(manager1, not(sameInstance(manager2)));

        // But same sort should still return the same manager
        var manager1Again = provider.getTopFieldCollectorManager(sort1);
        assertThat(manager1, sameInstance(manager1Again));
    }

    public void testLazyInitializationOfTopScoreDocCollectorManager() throws IOException {
        var factory = createFactory(100, true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // Manager should not be created yet
        assertThat(provider.topScoreDocCollectorManager, nullValue());

        // First access triggers initialization
        var manager = provider.getTopScoreDocCollectorManager();

        // Now it should be initialized
        assertThat(provider.topScoreDocCollectorManager, notNullValue());
        assertThat(provider.topScoreDocCollectorManager, sameInstance(manager));
    }

    public void testLazyInitializationOfTopFieldCollectorManager() throws IOException {
        var factory = createFactory(100, false, List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // Cache should be empty initially
        assertThat(provider.topFieldCollectorManagers.isEmpty(), equalTo(true));

        Sort sort = new Sort(SortField.FIELD_DOC);

        // First access triggers initialization
        var manager = provider.getTopFieldCollectorManager(sort);

        // Now it should be in the cache
        assertThat(provider.topFieldCollectorManagers.size(), equalTo(1));
        assertThat(provider.topFieldCollectorManagers.get(sort), sameInstance(manager));
    }

    public void testConcurrentAccessToTopScoreDocCollectorManager() throws Exception {
        var factory = createFactory(100, true, List.of(), DataPartitioning.SHARD);
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

    public void testConcurrentAccessToTopFieldCollectorManager() throws Exception {
        var factory = createFactory(100, false, List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);
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
        // Create providers directly to avoid resource leaks from creating multiple factories
        var provider1 = new LuceneTopNSourceOperator.PerShardCollectorProvider(100, true, List.of());
        var provider2 = new LuceneTopNSourceOperator.PerShardCollectorProvider(100, true, List.of());

        // Different providers should be different instances
        assertThat(provider1, not(sameInstance(provider2)));

        // And different underlying managers
        var manager1 = provider1.getTopScoreDocCollectorManager();
        var manager2 = provider2.getTopScoreDocCollectorManager();
        assertThat(manager1, not(sameInstance(manager2)));
    }

    public void testProviderWithoutScoreDoesNotCreateTopScoreDocCollectorManagerEagerly() throws IOException {
        var factory = createFactory(100, false, List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        // Should remain null since it hasn't been accessed yet (lazy initialization)
        assertThat(provider.topScoreDocCollectorManager, nullValue());

        // Accessing will create one (even with needsScore=false, the provider doesn't prevent this)
        // The actual usage logic prevents calling this method when needsScore=false
        var manager = provider.getTopScoreDocCollectorManager();
        assertThat(manager, notNullValue());
        assertThat(provider.topScoreDocCollectorManager, sameInstance(manager));
    }

    public void testNewPerShardCollectorCreatesDifferentCollectorInstances() throws IOException {
        var factory = createFactory(100, true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;
        var ctx = createMockShardContext(0);

        // Create multiple collectors
        var collector1 = provider.newPerShardCollector(ctx);
        var collector2 = provider.newPerShardCollector(ctx);

        // Each call should create a NEW collector instance
        assertThat(collector1, not(sameInstance(collector2)));
        assertThat(collector1.collector, not(sameInstance(collector2.collector)));
    }

    public void testNewTopDocsCollectorReusesManagersButCreatesNewCollectors() throws IOException {
        var factory = createFactory(100, true, List.of(), DataPartitioning.SHARD);
        var provider = factory.perShardCollectorProvider;

        Sort relevanceSort = Sort.RELEVANCE;

        // Create multiple collectors with the same sort
        var collector1 = provider.newTopDocsCollector(relevanceSort);
        var collector2 = provider.newTopDocsCollector(relevanceSort);
        var collector3 = provider.newTopDocsCollector(relevanceSort);

        // Each should be a different collector instance
        assertThat(collector1, not(sameInstance(collector2)));
        assertThat(collector2, not(sameInstance(collector3)));

        // But they should all come from the same manager (verify by checking the manager is reused)
        var manager1 = provider.getTopScoreDocCollectorManager();
        var manager2 = provider.getTopScoreDocCollectorManager();
        assertThat(manager1, sameInstance(manager2));
    }

    public void testMultipleOperatorsShareProvider() throws Exception {
        // Use needsScore=false since our buildSort() always returns field-based sorting
        var factory = createFactory(10, false, List.of(new FieldSortBuilder("s")), DataPartitioning.SHARD);

        int numOperators = 4;
        var operators = new ArrayList<LuceneTopNSourceOperator>();

        try {
            for (int i = 0; i < numOperators; i++) {
                operators.add((LuceneTopNSourceOperator) factory.get(createDriverContext()));
            }

            // Verify all share the same provider
            var sharedProvider = operators.get(0).perShardCollectorProvider;
            for (var op : operators) {
                assertThat(op.perShardCollectorProvider, sameInstance(sharedProvider));
            }

            // Verify the provider has the expected configuration
            assertThat(sharedProvider, notNullValue());

            // Access the TopFieldCollectorManager cache to verify it's shared
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

    private LuceneTopNSourceOperator.Factory createFactory(
        int limit,
        boolean needsScore,
        List<SortBuilder<?>> sorts,
        DataPartitioning partitioning
    ) throws IOException {
        setupIndex(100);
        ShardContext ctx = createMockShardContext(0);
        Function<ShardContext, List<LuceneSliceQueue.QueryAndTags>> queryFunction = c -> List.of(
            new LuceneSliceQueue.QueryAndTags(Queries.ALL_DOCS_INSTANCE, List.of())
        );

        return new LuceneTopNSourceOperator.Factory(
            new IndexedByShardIdFromSingleton<>(ctx),
            queryFunction,
            partitioning,
            LuceneSourceOperator.Factory::autoStrategy,
            1,
            10000,
            limit,
            sorts,
            8,
            needsScore
        );
    }

    private DriverContext createDriverContext() {
        var blockFactory = blockFactory();
        return new DriverContext(blockFactory.bigArrays(), blockFactory, null);
    }
}
