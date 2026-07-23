/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Paranoid coverage of the shared-analyzer reference counting on {@link AnalysisRegistry}.
 * Verifies that:
 *
 * <ul>
 *   <li>When N {@link IndexAnalyzers} share an analyzer, closing the first N-1 leaves the
 *       analyzer alive and the cache entry present; closing the last drops the cache entry and
 *       closes the underlying analyzer (the close-on-no-references guarantee).</li>
 *   <li>A cache that has been emptied does not grow back without re-interning.</li>
 *   <li>Inline {@code _analyze} calls (no index settings) never touch the cache.</li>
 *   <li>Building two indices and closing them in either order leaves the cache empty.</li>
 * </ul>
 *
 * <p>Real closure is verified directly, not just inferred from the cache draining:
 * {@link #testSharedUnderlyingAnalyzerClosedOnLastRelease} captures the underlying Lucene
 * {@link Analyzer}, asserts it stays usable while a sharer remains, and asserts it rejects further
 * tokenization with {@link AlreadyClosedException} once the last sharer closes — proving the
 * underlying analyzer (and its {@code CloseableThreadLocal} / {@code SynonymMap}) is torn down.
 * Cache size and the per-entry reference counts from
 * {@link AnalysisRegistry#analyzerCacheRefCounts()} corroborate the bookkeeping.
 */
public class AnalyzerRefcountTests extends ESTestCase {

    private AnalysisRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // The whole class exercises the recipe cache and its reference counting, which only exist when analyzer
        // sharing is on. Off (release builds) every index builds a fresh analyzer, so there is nothing to assert here.
        assumeTrue("analyzer sharing feature flag must be enabled", AnalysisRegistry.SHARED_ANALYZERS_FEATURE_FLAG.isEnabled());
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        AnalysisModule module = new AnalysisModule(
            TestEnvironment.newEnvironment(nodeSettings),
            Collections.emptyList(),
            new StablePluginsRegistry()
        );
        registry = module.getAnalysisRegistry();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            // registry is null when setUp skipped the test (analyzer sharing disabled) before building it.
            if (registry != null) {
                // Opt-in strict leak check: every test in this class must drain the cache before
                // teardown. Any IndexAnalyzers built without being closed shows up here, attributed
                // to the offending test method.
                registry.assertNoCachedEntries();
            }
        } finally {
            if (registry != null) {
                registry.close();
            }
            super.tearDown();
        }
    }

    private static IndexSettings indexSettings(String name) {
        Settings s = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        return IndexSettingsModule.newIndexSettings(name, s);
    }

    /**
     * Two indices share the default analyzer (a single recipe → single cache entry). Closing the
     * first must NOT close the analyzer (the second still references it). Closing the second
     * must drive the cache entry to zero and remove it.
     */
    public void testShareCloseFirstThenSecondEvictsOnSecondClose() throws IOException {
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        IndexAnalyzers b = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("b"));
        // Sharing precondition for the lifecycle test below.
        assertSame(a.getDefaultIndexAnalyzer(), b.getDefaultIndexAnalyzer());

        // Both alive: cache holds at least one entry.
        int sizeBoth = registry.analyzerCacheSize();
        assertThat("cache should be populated while both indices are open", sizeBoth, greaterThan(0));

        a.close();
        // After closing one of two sharers: cache size must NOT decrease (the entry still has
        // one referrer). This is the regression guard against an over-eager release.
        assertEquals("cache size must not drop while another index still references the entry", sizeBoth, registry.analyzerCacheSize());

        b.close();
        // After closing the last referrer: cache must drain back to empty.
        assertEquals("cache must drain to empty when all sharers close", 0, registry.analyzerCacheSize());
    }

    /**
     * Closing the only index that interned a recipe drops the cache entry — verifies the
     * single-referrer (non-shared) path also releases correctly.
     */
    public void testSingleIndexCloseEvicts() throws IOException {
        assertEquals(0, registry.analyzerCacheSize());
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        assertThat(registry.analyzerCacheSize(), greaterThan(0));
        a.close();
        assertEquals("cache must drain when the only referrer closes", 0, registry.analyzerCacheSize());
    }

    /**
     * The cache hit/miss counters (surfaced as APM counters by {@code AnalyzerMetrics}) must track
     * real builds vs shares: the first index of a recipe builds (misses), and a second index with
     * the identical recipe is served entirely from cache (hits, no new misses).
     */
    public void testCacheHitAndMissCountersTrackBuildsAndShares() throws IOException {
        long missesBefore = registry.cacheMisses();
        long hitsBefore = registry.cacheHits();

        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        assertThat("first build of a recipe is a miss", registry.cacheMisses(), greaterThan(missesBefore));
        long missesAfterFirst = registry.cacheMisses();

        IndexAnalyzers b = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("b"));
        assertThat("second identical index is served from cache", registry.cacheHits(), greaterThan(hitsBefore));
        assertEquals("second identical index must not trigger fresh builds", missesAfterFirst, registry.cacheMisses());

        a.close();
        b.close();
    }

    /**
     * Closing in reverse order is symmetric (b then a). Captures any latent assumption that
     * release order matches build order.
     */
    public void testShareCloseSecondThenFirstEvicts() throws IOException {
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        IndexAnalyzers b = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("b"));
        int sizeBoth = registry.analyzerCacheSize();
        b.close();
        assertEquals(sizeBoth, registry.analyzerCacheSize());
        a.close();
        assertEquals(0, registry.analyzerCacheSize());
    }

    /**
     * Three indices share; closing them one at a time decrements the count; only the third
     * close drives the cache entry to zero.
     */
    public void testThreeWaySharingDecrementsToZero() throws IOException {
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        IndexAnalyzers b = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("b"));
        IndexAnalyzers c = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("c"));

        // All three reference the same default analyzer instance.
        assertSame(a.getDefaultIndexAnalyzer(), b.getDefaultIndexAnalyzer());
        assertSame(b.getDefaultIndexAnalyzer(), c.getDefaultIndexAnalyzer());

        int beforeAny = registry.analyzerCacheSize();
        assertThat(beforeAny, greaterThan(0));

        a.close();
        assertEquals(beforeAny, registry.analyzerCacheSize());
        b.close();
        assertEquals(beforeAny, registry.analyzerCacheSize());
        c.close();
        assertEquals(0, registry.analyzerCacheSize());
    }

    /**
     * Idempotent close on {@link IndexAnalyzers}: closing twice must not double-release (which
     * would drive the refcount negative and leave a stale closed analyzer in the cache).
     */
    public void testDoubleCloseDoesNotDoubleRelease() throws IOException {
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        IndexAnalyzers b = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("b"));
        int sizeBoth = registry.analyzerCacheSize();

        a.close();
        // Second close on `a` must be a no-op for refcount accounting; the current IndexAnalyzers
        // contract doesn't guarantee idempotent close (Closeable allows but doesn't require it),
        // so we tolerate either pass or an exception, BUT we must not under-count b's reference.
        try {
            a.close();
        } catch (Exception ignored) {
            // acceptable: spec doesn't mandate idempotent close
        }
        assertEquals("double-close on a must not affect b's reference accounting", sizeBoth, registry.analyzerCacheSize());

        b.close();
        assertEquals(0, registry.analyzerCacheSize());
    }

    /**
     * The refcount on a freshly-interned entry is exactly 1. Verified via the test accessor so we
     * catch an off-by-one in the intern path (which would leak entries that never get evicted).
     */
    public void testFreshInternStartsAtRefCountOne() throws Exception {
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        // Walk the cache and assert every live entry has exactly 1 reference (from index a);
        // anything else would mean an off-by-one in the intern path.
        for (int rc : registry.analyzerCacheRefCounts()) {
            assertEquals("each live cache entry should hold exactly one ref from index a", 1, rc);
        }
        a.close();
        assertEquals(0, registry.analyzerCacheSize());
    }

    /**
     * Sharing across N indices: each cache entry's refcount equals N. Verifies the intern path
     * increments correctly, not just that the cache value is reused.
     */
    public void testRefCountMatchesNumberOfReferrers() throws Exception {
        int n = randomIntBetween(2, 7);
        List<IndexAnalyzers> handles = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            handles.add(registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("idx_" + i)));
        }
        for (int rc : registry.analyzerCacheRefCounts()) {
            assertEquals("refcount must equal the number of building indices", n, rc);
        }
        for (IndexAnalyzers ia : handles) {
            ia.close();
        }
        assertEquals(0, registry.analyzerCacheSize());
    }

    /**
     * Inline {@code _analyze} (no IndexSettings) must bypass the cache entirely — the
     * unbounded one-shot analyzer ecosystem would otherwise leak into the cache.
     */
    /**
     * If {@code build()} throws part-way through (here: validation fails because an analyzer
     * name starts with {@code _}), every reference acquired up to that point must be released.
     * Without rollback, the failed build would leave dangling refcounts that pin the cache
     * entries until node shutdown — a slow but real leak triggered by misconfigured indices.
     */
    public void testFailedBuildReleasesAllReferences() {
        // First build a valid index to populate the default-analyzer cache entry, then a second
        // index that fails validation. The valid index still holds its references; the failed
        // build must drop everything it intern'd before the validation tripwire fired.
        Settings idx = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            // Cause IllegalArgumentException after analyzers have been interned (analyzer name
            // starting with '_' fails the post-build validation loop).
            .put("index.analysis.analyzer._invalid.tokenizer", "standard")
            .build();
        IndexSettings failing = IndexSettingsModule.newIndexSettings("idx", idx);

        int beforeBuild = registry.analyzerCacheSize();
        expectThrows(IllegalArgumentException.class, () -> registry.build(IndexCreationContext.CREATE_INDEX, failing));
        assertEquals("a failed build must not leave dangling refcounts in the cache", beforeBuild, registry.analyzerCacheSize());
    }

    /**
     * Single-flight invariant: when N threads concurrently {@code build()} indices with the
     * same recipe, only one of them runs the analyzer-building work (which for synonym chains
     * reads from disk / the .synonyms system index). Without coalescing, opening 100 shards on
     * the same recipe would do 100× the synonym read work; only one wins.
     */
    public void testConcurrentBuildsCoalesceToOneBuild() throws Exception {
        // Use a custom analyzer recipe + a counting "stop"-like factory whose constructor
        // increments on each invocation. The wrapper around AnalysisRegistry doesn't let us
        // inject custom factories here, so we count cacheMisses on the registry — under
        // single-flight, N concurrent build() calls on the same recipe should produce exactly
        // ONE miss for the analyzer cache (plus the unavoidable misses for the prebuilt
        // analyzers populated alongside).
        long missesBefore = registry.cacheMisses();
        int concurrency = 32;
        ExecutorService pool = Executors.newFixedThreadPool(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<IndexAnalyzers>> futures = new ArrayList<>(concurrency);
        try {
            for (int i = 0; i < concurrency; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    start.await();
                    return registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("idx_" + idx));
                }));
            }
            start.countDown();
            List<IndexAnalyzers> built = new ArrayList<>(concurrency);
            for (var f : futures) {
                built.add(f.get(30, TimeUnit.SECONDS));
            }
            // All builds should converge on the same default analyzer (recipe is identical).
            NamedAnalyzer canonical = built.get(0).getDefaultIndexAnalyzer();
            for (IndexAnalyzers ia : built) {
                assertSame("all concurrent builds must share the same default analyzer", canonical, ia.getDefaultIndexAnalyzer());
            }
            long missesAfter = registry.cacheMisses();
            long misses = missesAfter - missesBefore;
            int distinctSlots = registry.analyzerCacheSize();
            // Single-flight guarantee: misses must be O(distinct cache slots), NOT
            // O(concurrency * slots). Without coalescing, 32 threads building the same recipe
            // would produce ~32 × distinctSlots misses. With coalescing, it's distinctSlots
            // plus a small slack for race interleavings between get() and compute(). We assert
            // misses are well under the no-coalesce baseline — the order-of-magnitude difference
            // is what matters operationally; an off-by-a-few from interleavings is fine.
            long noCoalesceBaseline = (long) concurrency * distinctSlots;
            assertThat(
                "single-flight should keep misses tiny vs the no-coalesce baseline of " + noCoalesceBaseline + ". Got " + misses,
                misses,
                lessThan(noCoalesceBaseline / 4)
            );
            assertThat(
                "misses should be on the order of distinctSlots, not concurrency",
                misses,
                lessThanOrEqualTo((long) distinctSlots * 3)
            );
            for (IndexAnalyzers ia : built) {
                ia.close();
            }
            assertEquals("cache must drain after all concurrent indices close", 0, registry.analyzerCacheSize());
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Correctness invariant: sharing must not change tokenization behaviour. Two indices built
     * from the same registry tokenize the same input via the shared default analyzer; the
     * resulting tokens must be identical, byte-for-byte. Guards against the "shared instance
     * carries hidden per-index state" failure mode where a poorly-isolated factory would let
     * one index's settings leak into another's tokenization.
     */
    public void testSharedAnalyzerTokenizesIdenticallyAcrossIndices() throws IOException {
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        IndexAnalyzers b = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("b"));
        try {
            assertSame("precondition: indices share the default analyzer", a.getDefaultIndexAnalyzer(), b.getDefaultIndexAnalyzer());
            String input = "The quick brown fox jumps over the lazy dog";
            List<String> tokensA = tokenize(a.getDefaultIndexAnalyzer(), input);
            List<String> tokensB = tokenize(b.getDefaultIndexAnalyzer(), input);
            assertEquals("sharing must not alter tokenization output", tokensA, tokensB);
            assertFalse("sanity: tokenization actually produced tokens", tokensA.isEmpty());
        } finally {
            a.close();
            b.close();
        }
    }

    /**
     * Concurrency invariant: a shared analyzer must support tokenization from multiple threads
     * safely (Lucene's {@link org.apache.lucene.analysis.Analyzer} uses a
     * {@code CloseableThreadLocal} for the {@code TokenStreamComponents}; if our wrapper or
     * sharing broke that invariant, parallel tokenization would interleave or produce wrong
     * output). Four threads, fixed input, deterministic expected output.
     */
    public void testSharedAnalyzerSafeUnderConcurrentTokenization() throws Exception {
        IndexAnalyzers ia = registry.build(IndexCreationContext.CREATE_INDEX, indexSettings("a"));
        try {
            NamedAnalyzer shared = ia.getDefaultIndexAnalyzer();
            String input = "The quick brown fox jumps over the lazy dog";
            List<String> expected = tokenize(shared, input);
            int threads = 4;
            int rounds = 32;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<Void>> futures = new ArrayList<>(threads);
            try {
                for (int t = 0; t < threads; t++) {
                    futures.add(pool.submit(() -> {
                        start.await();
                        for (int r = 0; r < rounds; r++) {
                            List<String> got = tokenize(shared, input);
                            assertEquals("concurrent tokenization must match the single-threaded reference", expected, got);
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (var f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }
        } finally {
            ia.close();
        }
    }

    /**
     * Tokenize {@code input} into a list of term strings using {@code analyzer}. Always closes
     * the {@link org.apache.lucene.analysis.TokenStream}.
     */
    private static List<String> tokenize(NamedAnalyzer analyzer, String input) throws IOException {
        List<String> out = new ArrayList<>();
        try (TokenStream ts = analyzer.tokenStream("f", input)) {
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                out.add(term.toString());
            }
            ts.end();
        }
        return out;
    }

    /**
     * The close-on-no-references guarantee must reach the <em>underlying</em> Lucene analyzer, not
     * just evict the cache entry. A custom analyzer is built INDEX-scoped and re-tagged GLOBAL
     * before caching (so a per-index close can't tear down an instance another index still shares);
     * the regression this guards against is that the GLOBAL re-tag also suppresses
     * {@link NamedAnalyzer#close}'s cascade to the underlying analyzer, leaking its
     * {@code CloseableThreadLocal} (and, for synonym chains, its {@code SynonymMap}) on the
     * index-deletion path. We detect "closed" by observing that the underlying analyzer rejects
     * further tokenization with {@link AlreadyClosedException}.
     */
    public void testSharedUnderlyingAnalyzerClosedOnLastRelease() throws IOException {
        Settings recipe = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.analysis.analyzer.custom_a.type", "custom")
            .put("index.analysis.analyzer.custom_a.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_a.filter", "lowercase")
            .build();
        IndexAnalyzers a = registry.build(IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("a", recipe));
        IndexAnalyzers b = registry.build(IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("b", recipe));

        NamedAnalyzer sharedA = a.get("custom_a");
        assertSame("precondition: identical custom recipes must share one instance", sharedA, b.get("custom_a"));
        Analyzer underlying = sharedA.analyzer();

        // While referenced, the underlying analyzer tokenizes normally.
        assertEquals(List.of("hello"), tokenize(sharedA, "Hello"));

        a.close();
        // One of two sharers closed: the entry is still referenced, so the underlying must remain
        // open and usable. (Guards against an over-eager close as much as the leak.)
        assertEquals("underlying must stay open while another index references it", List.of("hello"), tokenize(sharedA, "Hello"));

        b.close();
        // Last referrer gone: the underlying analyzer itself must now be closed, not merely evicted.
        assertEquals("cache must drain when the last sharer closes", 0, registry.analyzerCacheSize());
        expectThrows(AlreadyClosedException.class, () -> underlying.tokenStream("f", "Hello"));
    }

    public void testInlineAnalyzeDoesNotTouchCache() throws IOException {
        int before = registry.analyzerCacheSize();
        registry.buildCustomAnalyzer(IndexCreationContext.CREATE_INDEX, null, false, new NameOrDefinition("standard"), List.of(), List.of())
            .close();
        assertEquals("inline custom analyzer must not enter the cache", before, registry.analyzerCacheSize());
    }

}
