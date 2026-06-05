/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersRequest;
import org.elasticsearch.action.admin.indices.analyze.ReloadAnalyzersResponse;
import org.elasticsearch.action.admin.indices.analyze.TransportReloadAnalyzersAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

/**
 * End-to-end coverage of node-level analyzer sharing: two indices created through the real index
 * lifecycle that declare an identical custom analyzer recipe must resolve to the very same
 * {@link NamedAnalyzer} instance, and a shared analyzer must outlive deletion of one of its
 * sharers (the reference-counted cache only closes it once the last referencing index is gone).
 */
public class AnalyzerSharingIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(CommonAnalysisPlugin.class);
    }

    private static Settings recipe(String stopword) {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.filter.my_stop.type", "stop")
            .putList("index.analysis.filter.my_stop.stopwords", stopword)
            .put("index.analysis.analyzer.a.tokenizer", "standard")
            .putList("index.analysis.analyzer.a.filter", "lowercase", "my_stop")
            .build();
    }

    public void testIdenticalRecipesShareAnalyzerInstance() {
        IndexService a = createIndex("index_a", recipe("the"));
        IndexService b = createIndex("index_b", recipe("the"));
        assertSame(
            "two indices with an identical analyzer recipe must share one analyzer instance",
            a.getIndexAnalyzers().get("a"),
            b.getIndexAnalyzers().get("a")
        );
    }

    public void testDifferentRecipesDoNotShare() {
        IndexService a = createIndex("index_a", recipe("the"));
        IndexService c = createIndex("index_c", recipe("and"));
        assertNotSame(
            "indices whose analyzer recipe differs must not share an analyzer",
            a.getIndexAnalyzers().get("a"),
            c.getIndexAnalyzers().get("a")
        );
    }

    /**
     * Integration regression test for the reload-ordering bug (B3): when two indices share an
     * analyzer under different local names, a {@code _reload_search_analyzers} call listing the
     * "sharer" index (whose local name differs from the cached entry's name) first must still
     * reload both indices.
     *
     * <p>With the bug present:
     * <ol>
     *   <li>index_a's shard processes first, claims the reload token, then looks up settings via
     *       the cached analyzer's name ({@code "search_z"}) in index_a's settings → {@code null}
     *       → silent bail-out.
     *   <li>index_z's shard arrives with the same token → {@code tryClaimReload} returns
     *       {@code false} → also bails.
     *   <li>Response reports success; no synonyms were reloaded.
     * </ol>
     *
     * <p>Setup ordering that deterministically triggers the bug:
     * index_z is created first so its analyzer name owns the cache entry. index_a is created
     * second and shares the cached instance. The reload request lists index_a before index_z,
     * causing index_a's shard to process first.
     */
    public void testReloadSharedSynonymsBothIndicesSeeUpdate() throws IOException {
        final String synFileName = "syn_sharing_it.txt";
        Path configDir = node().getEnvironment().configDir();
        if (Files.exists(configDir) == false) {
            Files.createDirectory(configDir);
        }
        Path synFile = configDir.resolve(synFileName);
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(
                    Files.newOutputStream(synFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING),
                    StandardCharsets.UTF_8
                )
            )
        ) {
            out.println("hello, hi");
        }

        // index_z created FIRST — its analyzer name "search_z" owns the shared cache entry.
        assertAcked(
            indicesAdmin().prepareCreate("index_z")
                .setSettings(
                    indexSettings(1, 0).put("analysis.filter.syn.type", "synonym")
                        .put("analysis.filter.syn.synonyms_path", synFileName)
                        .put("analysis.filter.syn.updateable", true)
                        .put("analysis.analyzer.search_z.tokenizer", "standard")
                        .putList("analysis.analyzer.search_z.filter", "lowercase", "syn")
                )
        );

        // index_a created SECOND — identical recipe, different local analyzer name.
        // Shares the cached NamedAnalyzer("search_z", ...) via the registry.
        assertAcked(
            indicesAdmin().prepareCreate("index_a")
                .setSettings(
                    indexSettings(1, 0).put("analysis.filter.syn.type", "synonym")
                        .put("analysis.filter.syn.synonyms_path", synFileName)
                        .put("analysis.filter.syn.updateable", true)
                        .put("analysis.analyzer.search_a.tokenizer", "standard")
                        .putList("analysis.analyzer.search_a.filter", "lowercase", "syn")
                )
        );

        prepareIndex("index_z").setId("1").setSource("content", "hello").get();
        prepareIndex("index_a").setId("1").setSource("content", "hello").get();
        assertNoFailures(indicesAdmin().prepareRefresh("index_z", "index_a").get());

        // Add "hey" as a new synonym so after reload "hey" should match "hello" documents.
        try (
            PrintWriter out = new PrintWriter(
                new OutputStreamWriter(
                    Files.newOutputStream(synFile, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING),
                    StandardCharsets.UTF_8
                )
            )
        ) {
            out.println("hello, hi, hey");
        }

        // Reload with index_a listed FIRST (the ordering that triggers the B3 bug).
        ReloadAnalyzersResponse reloadResponse = client().execute(
            TransportReloadAnalyzersAction.TYPE,
            new ReloadAnalyzersRequest(null, false, "index_a", "index_z")
        ).actionGet();
        assertNoFailures(reloadResponse);

        // After reload both indices must expand "hey" to "hello" at search time.
        // With the B3 bug the reload is silently skipped and both hit counts are 0.
        assertHitCount(client().prepareSearch("index_z").setQuery(QueryBuilders.matchQuery("content", "hey").analyzer("search_z")), 1L);
        assertHitCount(client().prepareSearch("index_a").setQuery(QueryBuilders.matchQuery("content", "hey").analyzer("search_a")), 1L);
    }

    /**
     * Regression test for the single-flight coalescing path in the analyzer cache: when N index
     * creation requests with an identical analyzer recipe are submitted concurrently, all resulting
     * indices must resolve to the very same {@link NamedAnalyzer} instance. Only one builder should
     * materialise; the rest join the in-flight {@code CompletableFuture}.
     *
     * <p>Note: in a single-node setup, cluster-state updates are serialised on the master-service
     * thread, so the actual analyzer builds also execute sequentially. The test still validates the
     * end-to-end sharing invariant under concurrent API usage and is a regression guard against
     * accidental cache invalidation or key-equality regressions.
     */
    public void testConcurrentCreationWithIdenticalRecipeSharesInstance() throws Exception {
        final int n = 4;
        final Settings s = recipe("the");
        final CountDownLatch ready = new CountDownLatch(n);
        final CountDownLatch go = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();

        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            final String indexName = "index_concurrent_" + i;
            threads[i] = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    assertAcked(indicesAdmin().prepareCreate(indexName).setSettings(s));
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
            });
        }
        for (Thread t : threads)
            t.start();
        assertTrue("threads did not get ready in time", ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        for (Thread t : threads)
            t.join(30_000);

        assertNull("concurrent creation threw: " + failure.get(), failure.get());

        // Collect NamedAnalyzer instances by iterating IndicesService (avoids cluster-state API version skew).
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NamedAnalyzer first = null;
        int found = 0;
        for (IndexService indexService : indicesService) {
            if (indexService.index().getName().startsWith("index_concurrent_")) {
                NamedAnalyzer na = indexService.getIndexAnalyzers().get("a");
                if (first == null) {
                    first = na;
                } else {
                    assertSame("all concurrently-created indices must share one NamedAnalyzer instance", first, na);
                }
                found++;
            }
        }
        assertEquals("expected all " + n + " concurrent indices to be found", n, found);
    }

    public void testSharedAnalyzerSurvivesDeletionOfOneSharer() throws IOException {
        createIndex("index_a", recipe("the"));
        IndexService b = createIndex("index_b", recipe("the"));
        NamedAnalyzer shared = b.getIndexAnalyzers().get("a");

        assertAcked(indicesAdmin().prepareDelete("index_a"));

        // index_b still references the shared analyzer, so it must not have been closed when
        // index_a (the other sharer) was deleted — exercising the refcounted release path.
        assertSame(shared, b.getIndexAnalyzers().get("a"));
        try (TokenStream ts = shared.tokenStream("f", "The Quick Brown Fox")) {
            ts.reset();
            while (ts.incrementToken()) {
            }
            ts.end();
        }
    }
}
