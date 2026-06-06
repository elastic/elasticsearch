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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;

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

    /** Same recipe, but the analyzer carries a different local name in each index. */
    private static Settings recipeNamed(String analyzerName, String stopword) {
        return Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.analysis.filter.my_stop.type", "stop")
            .putList("index.analysis.filter.my_stop.stopwords", stopword)
            .put("index.analysis.analyzer." + analyzerName + ".tokenizer", "standard")
            .putList("index.analysis.analyzer." + analyzerName + ".filter", "lowercase", "my_stop")
            .build();
    }

    /** A {@code content} text field whose analyzer is the custom analyzer named {@code analyzerName}. */
    private static XContentBuilder contentFieldMapping(String analyzerName) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("content")
            .field("type", "text")
            .field("analyzer", analyzerName)
            .endObject()
            .endObject()
            .endObject();
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
     * Two indices declare a byte-for-byte identical recipe but name the analyzer differently
     * ({@code ana} vs {@code anb}) and use it as a field's analyzer. Sharing keys on the recipe, so
     * both fields are backed by one shared analyzer instance — but the {@link NamedAnalyzer} wrapper
     * handed to each index keeps that index's OWN local name. {@code FieldMapper} serializes the
     * analyzer via {@code NamedAnalyzer.name()}, and {@code MapperService} assertSerialization (run
     * under {@code -ea}) round-trips that mapping against the index's own settings, so each index's
     * mapping must reference the name it actually configured. Creating the second index exercises
     * that round-trip; the explicit checks below document the intent.
     */
    public void testSharedAnalyzerKeepsPerIndexLocalNameInMappings() throws IOException {
        IndexService a = createIndex("index_a", recipeNamed("ana", "the"), contentFieldMapping("ana"));
        // index_b shares index_a's underlying analyzer but under its own local name "anb"; its content
        // field serializes "anb", which round-trips against index_b's settings.
        IndexService b = createIndex("index_b", recipeNamed("anb", "the"), contentFieldMapping("anb"));

        NamedAnalyzer ana = a.getIndexAnalyzers().get("ana");
        NamedAnalyzer anb = b.getIndexAnalyzers().get("anb");
        assertSame("identical recipes must share the underlying analyzer", ana.analyzer(), anb.analyzer());
        assertEquals("index_a's wrapper must keep its own local name", "ana", ana.name());
        assertEquals("index_b's wrapper must keep its own local name, not the shared builder's", "anb", anb.name());

        // The field mappers must serialize each index's own analyzer name, so the mapping round-trips.
        String mappingSourceA = a.mapperService().documentMapper().mappingSource().toString();
        String mappingSourceB = b.mapperService().documentMapper().mappingSource().toString();
        assertThat(mappingSourceA, containsString("\"analyzer\":\"ana\""));
        assertThat(mappingSourceB, containsString("\"analyzer\":\"anb\""));
    }

    /**
     * When two indices share one synonym analyzer under different local names, a single
     * {@code _reload_search_analyzers} call must refresh the synonyms for both — regardless of which
     * index the request lists first and which index's name owns the shared cache entry.
     *
     * <p>The setup makes the names diverge deliberately: index_z is created first so its analyzer name
     * ({@code search_z}) owns the cached instance, index_a is created second and shares it under its
     * own name ({@code search_a}), and the reload lists index_a before index_z. After the reload both
     * indices expand the newly added synonym {@code hey} at search time.
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

        // Reload with index_a (the sharer, whose local name differs from the cache entry's) listed first.
        ReloadAnalyzersResponse reloadResponse = client().execute(
            TransportReloadAnalyzersAction.TYPE,
            new ReloadAnalyzersRequest(null, false, "index_a", "index_z")
        ).actionGet();
        assertNoFailures(reloadResponse);

        // After reload both indices expand "hey" to "hello" at search time.
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

    /**
     * Concurrency robustness: firing {@code _reload_search_analyzers} and deleting the two indices that
     * share the reloaded analyzer at the same time must complete cleanly. When the last sharer's
     * refcount reaches 0 the shared {@link org.elasticsearch.index.analysis.ReloadableCustomAnalyzer}
     * is closed; this exercises that teardown racing an in-flight reload of the same instance.
     *
     * <p>An {@code IndexNotFoundException} from the reload is expected (the indices may already be
     * gone); any other exception fails the test. The loop count is randomised between 10 and 20 to
     * vary scheduling.
     */
    public void testConcurrentReloadAndIndexDeletion() throws Exception {
        final String synFileName = "syn_race_test.txt";
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

        final int iterations = randomIntBetween(10, 20);
        for (int i = 0; i < iterations; i++) {
            final String indexX = "index_x_" + i;
            final String indexY = "index_y_" + i;

            // Both indices share the same synonym-based analyzer recipe.
            assertAcked(
                indicesAdmin().prepareCreate(indexX)
                    .setSettings(
                        indexSettings(1, 0).put("analysis.filter.syn.type", "synonym")
                            .put("analysis.filter.syn.synonyms_path", synFileName)
                            .put("analysis.filter.syn.updateable", true)
                            .put("analysis.analyzer.search_x.tokenizer", "standard")
                            .putList("analysis.analyzer.search_x.filter", "lowercase", "syn")
                    )
            );
            assertAcked(
                indicesAdmin().prepareCreate(indexY)
                    .setSettings(
                        indexSettings(1, 0).put("analysis.filter.syn.type", "synonym")
                            .put("analysis.filter.syn.synonyms_path", synFileName)
                            .put("analysis.filter.syn.updateable", true)
                            .put("analysis.analyzer.search_y.tokenizer", "standard")
                            .putList("analysis.analyzer.search_y.filter", "lowercase", "syn")
                    )
            );

            prepareIndex(indexX).setId("1").setSource("content", "hello").get();
            prepareIndex(indexY).setId("1").setSource("content", "hello").get();
            assertNoFailures(indicesAdmin().prepareRefresh(indexX, indexY).get());

            // Latch so reload and delete race as tightly as possible.
            final CountDownLatch startGun = new CountDownLatch(1);
            final AtomicReference<Exception> reloadError = new AtomicReference<>();

            final String capturedX = indexX;
            final String capturedY = indexY;
            Thread reloadThread = new Thread(() -> {
                try {
                    startGun.await();
                    client().execute(TransportReloadAnalyzersAction.TYPE, new ReloadAnalyzersRequest(null, false, capturedX, capturedY))
                        .actionGet();
                } catch (Exception e) {
                    // IndexNotFoundException and ResourceNotFoundException are expected when the
                    // indices have already been deleted before or during the reload. Any other
                    // exception (e.g. AlreadyClosedException, NPE) is an unexpected failure.
                    String msg = e.getClass().getName() + ": " + e.getMessage();
                    if (msg.contains("index_not_found") == false
                        && msg.contains("IndexNotFoundException") == false
                        && msg.contains("no such index") == false) {
                        reloadError.set(e);
                    }
                }
            });
            reloadThread.start();

            // Fire reload and delete as simultaneously as possible.
            startGun.countDown();
            try {
                indicesAdmin().prepareDelete(indexX, indexY).get();
            } catch (Exception e) {
                // Indices may already be gone; that is fine.
            }

            reloadThread.join(30_000);
            assertFalse("reload thread did not finish within 30s", reloadThread.isAlive());
            assertNull("iteration " + i + ": unexpected exception from concurrent reload+delete: " + reloadError.get(), reloadError.get());
        }
    }

    /**
     * A shared synonym analyzer is loaded from its resources once per node, not once per index that
     * shares it. The initial load is deferred from build time to shard recovery
     * ({@link IndicesService}'s {@code beforeIndexShardRecovery}, a null-token
     * reload); once the shared instance is loaded, a second index with an identical recipe attaches to
     * it and its own shard recovery does NOT rebuild it.
     *
     * <p>Concretely: index_a loads {@code hello, hi}. The file is then changed to add {@code hey}, and
     * index_b is created with the same recipe. index_b shares index_a's already-loaded instance and does
     * not re-read the file, so neither index sees {@code hey} yet — creating an index never silently
     * mutates the synonyms other indices are already using. An explicit
     * {@code _reload_search_analyzers} then reloads the shared instance once and converges both indices.
     */
    public void testSecondIndexSharesLoadedAnalyzerWithoutRebuilding() throws IOException {
        final String synFileName = "syn_stale_test.txt";
        Path configDir = node().getEnvironment().configDir();
        if (Files.exists(configDir) == false) {
            Files.createDirectory(configDir);
        }
        Path synFile = configDir.resolve(synFileName);

        // Step 1: write initial synonym file with "hello, hi".
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

        // Step 2: create index_a — this builds and caches the analyzer with the initial synonyms.
        // Shard recovery reloads with the same file content: "hello, hi".
        assertAcked(
            indicesAdmin().prepareCreate("index_a")
                .setSettings(
                    indexSettings(1, 0).put("analysis.filter.syn.type", "synonym")
                        .put("analysis.filter.syn.synonyms_path", synFileName)
                        .put("analysis.filter.syn.updateable", true)
                        .put("analysis.analyzer.search_syn.tokenizer", "standard")
                        .putList("analysis.analyzer.search_syn.filter", "lowercase", "syn")
                )
        );

        prepareIndex("index_a").setId("1").setSource("content", "hello").get();
        assertNoFailures(indicesAdmin().prepareRefresh("index_a").get());

        // index_a has the initial synonyms: "hi" matches, "hey" does not.
        assertHitCount(client().prepareSearch("index_a").setQuery(QueryBuilders.matchQuery("content", "hi").analyzer("search_syn")), 1L);
        assertHitCount(client().prepareSearch("index_a").setQuery(QueryBuilders.matchQuery("content", "hey").analyzer("search_syn")), 0L);

        // Step 3: update the synonym file to add "hey" as a new synonym.
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

        // Step 4: create index_b with identical recipe. Because updateable synonyms key on path only,
        // index_b joins index_a's cache entry and shares the same ReloadableCustomAnalyzer. Its shard
        // recovery fires a null-token reload, but the shared instance is already loaded, so the claim is
        // refused and the analyzer is NOT rebuilt — the updated file is not read on index creation.
        assertAcked(
            indicesAdmin().prepareCreate("index_b")
                .setSettings(
                    indexSettings(1, 0).put("analysis.filter.syn.type", "synonym")
                        .put("analysis.filter.syn.synonyms_path", synFileName)
                        .put("analysis.filter.syn.updateable", true)
                        .put("analysis.analyzer.search_syn.tokenizer", "standard")
                        .putList("analysis.analyzer.search_syn.filter", "lowercase", "syn")
                )
        );

        prepareIndex("index_b").setId("1").setSource("content", "hello").get();
        assertNoFailures(indicesAdmin().prepareRefresh("index_b").get());

        // Verify that both indices share the same underlying NamedAnalyzer instance.
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        NamedAnalyzer analyzerA = null;
        NamedAnalyzer analyzerB = null;
        for (IndexService indexService : indicesService) {
            if (indexService.index().getName().equals("index_a")) {
                analyzerA = indexService.getIndexAnalyzers().get("search_syn");
            } else if (indexService.index().getName().equals("index_b")) {
                analyzerB = indexService.getIndexAnalyzers().get("search_syn");
            }
        }
        assertNotNull("index_a must be present", analyzerA);
        assertNotNull("index_b must be present", analyzerB);
        assertSame(
            "index_b must share the cached analyzer instance from index_a (identical recipe, path-only key for updateable synonyms)",
            analyzerA,
            analyzerB
        );

        // index_b did not rebuild the shared (already-loaded) analyzer, so neither index sees the new
        // "hey" synonym yet — index creation must not silently re-read the file and change the synonyms
        // other indices are already using. "hi" still matches on both.
        assertHitCount(client().prepareSearch("index_a").setQuery(QueryBuilders.matchQuery("content", "hey").analyzer("search_syn")), 0L);
        assertHitCount(client().prepareSearch("index_b").setQuery(QueryBuilders.matchQuery("content", "hey").analyzer("search_syn")), 0L);
        assertHitCount(client().prepareSearch("index_a").setQuery(QueryBuilders.matchQuery("content", "hi").analyzer("search_syn")), 1L);
        assertHitCount(client().prepareSearch("index_b").setQuery(QueryBuilders.matchQuery("content", "hi").analyzer("search_syn")), 1L);

        // An explicit reload (non-null token) does rebuild the shared instance once, converging both.
        assertNoFailures(
            client().execute(TransportReloadAnalyzersAction.TYPE, new ReloadAnalyzersRequest(null, false, "index_a", "index_b")).actionGet()
        );
        assertHitCount(client().prepareSearch("index_a").setQuery(QueryBuilders.matchQuery("content", "hey").analyzer("search_syn")), 1L);
        assertHitCount(client().prepareSearch("index_b").setQuery(QueryBuilders.matchQuery("content", "hey").analyzer("search_syn")), 1L);
    }

    /**
     * Documents that when N indices concurrently attempt to create an analyzer with the same recipe
     * and the builder fails (e.g., the synonym file does not exist), ALL N creations fail — not just
     * the one that triggered the build. The single-flight {@link java.util.concurrent.CompletableFuture}
     * in {@code AnalysisRegistry.lookupOrIntern} fans the exception out to every joiner via
     * {@code completeExceptionally}, so all concurrent callers receive the failure.
     *
     * <p>After the concurrent failures the cache entry is properly retired ({@code cache.remove} is
     * called by the failing builder). A subsequent single-threaded attempt with the same nonexistent
     * file also fails cleanly — confirming the entry was removed and the next attempt retries from
     * scratch rather than hanging on a permanently-failed future.
     */
    public void testConcurrentIndexCreationWithFailingBuilderFansOutError() throws Exception {
        final String nonexistentFile = "nonexistent_synonym_file.txt";
        final int n = 4;
        final Settings s = indexSettings(1, 0).put("analysis.filter.syn.type", "synonym")
            .put("analysis.filter.syn.synonyms_path", nonexistentFile)
            .put("analysis.filter.syn.updateable", true)
            .put("analysis.analyzer.search_fail.tokenizer", "standard")
            .putList("analysis.analyzer.search_fail.filter", "lowercase", "syn")
            .build();

        final CountDownLatch ready = new CountDownLatch(n);
        final CountDownLatch go = new CountDownLatch(1);
        final AtomicInteger failureCount = new AtomicInteger(0);

        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            final String indexName = "index_fail_" + i;
            threads[i] = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                    indicesAdmin().prepareCreate(indexName).setSettings(s).get();
                    // If creation unexpectedly succeeds, that is a test failure recorded below.
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                }
            });
        }
        for (Thread t : threads) {
            t.start();
        }
        assertTrue("threads did not get ready in time", ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        for (Thread t : threads) {
            t.join(30_000);
        }

        assertEquals("all " + n + " concurrent index creations must fail when the synonym file does not exist", n, failureCount.get());

        // After the concurrent failures the cache entry must have been retired. A fresh
        // single-threaded attempt with the same (still-nonexistent) file must also fail cleanly —
        // not hang or deadlock — confirming the entry was properly removed from the cache.
        Exception singleThreadFailure = null;
        try {
            indicesAdmin().prepareCreate("index_fail_single").setSettings(s).get();
        } catch (Exception e) {
            singleThreadFailure = e;
        }
        assertNotNull("single-threaded retry after concurrent failures must also fail (file still does not exist)", singleThreadFailure);
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
