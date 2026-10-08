/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.index.analysis.AnalyzerComponents.createComponents;

public class ReloadableCustomAnalyzerTests extends ESTestCase {

    private static TestAnalysis testAnalysis;
    private static Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();

    private static TokenFilterFactory NO_OP_SEARCH_TIME_FILTER = new AbstractTokenFilterFactory("my_filter") {
        @Override
        public AnalysisMode getAnalysisMode() {
            return AnalysisMode.SEARCH_TIME;
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return tokenStream;
        }

        @Override
        public Object sharingKey() {
            return this;
        }
    };

    private static TokenFilterFactory LOWERCASE_SEARCH_TIME_FILTER = new AbstractTokenFilterFactory("my_other_filter") {
        @Override
        public AnalysisMode getAnalysisMode() {
            return AnalysisMode.SEARCH_TIME;
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return new LowerCaseFilter(tokenStream);
        }

        @Override
        public Object sharingKey() {
            return this;
        }
    };

    @BeforeClass
    public static void setup() throws IOException {
        testAnalysis = createTestAnalysis(new Index("test", "_na_"), settings);
    }

    /**
     * test constructor and getters
     */
    public void testBasicCtor() {
        int positionIncrementGap = randomInt();
        int offsetGap = randomInt();

        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();

        AnalyzerComponents components = createComponents(
            IndexCreationContext.CREATE_INDEX,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );

        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(components, positionIncrementGap, offsetGap)) {
            assertEquals(positionIncrementGap, analyzer.getPositionIncrementGap(randomAlphaOfLength(5)));
            assertEquals(offsetGap >= 0 ? offsetGap : 1, analyzer.getOffsetGap(randomAlphaOfLength(5)));
            assertEquals("standard", analyzer.getComponents().getTokenizerFactory().name());
            assertEquals(0, analyzer.getComponents().getCharFilters().length);
            assertSame(testAnalysis.tokenizer.get("standard"), analyzer.getComponents().getTokenizerFactory());
            assertEquals(1, analyzer.getComponents().getTokenFilters().length);
            assertSame(NO_OP_SEARCH_TIME_FILTER, analyzer.getComponents().getTokenFilters()[0]);
        }

        // check that when using regular non-search time filters only, we get an exception
        final Settings indexAnalyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "lowercase").build();
        AnalyzerComponents indexAnalyzerComponents = createComponents(
            IndexCreationContext.CREATE_INDEX,
            "my_analyzer",
            indexAnalyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            testAnalysis.tokenFilter
        );
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ReloadableCustomAnalyzer(indexAnalyzerComponents, positionIncrementGap, offsetGap)
        );
        assertEquals(
            "ReloadableCustomAnalyzer must only be initialized with analysis components in AnalysisMode.SEARCH_TIME mode",
            ex.getMessage()
        );
    }

    /**
     * The initial resource load of a shared analyzer (the null-token reload fired by shard recovery)
     * happens once per node: the first claim succeeds, and once the instance is loaded later null-token
     * claims are refused so a new shard opening does not rebuild an already-loaded shared analyzer. An
     * explicit reload request (non-null token) always reloads regardless.
     */
    public void testNullTokenReloadLoadsOnce() throws IOException {
        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents initial = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );
        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(initial, 0, 0)) {
            assertTrue("the initial recovery load is needed", analyzer.shouldReload(null));
            analyzer.reload(null, "my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter, noOp());
            AnalyzerComponents afterInitialLoad = analyzer.getComponents();

            // A later recovery (null-token) reload is a no-op — neither the hint nor the reload rebuilds.
            assertFalse("a later recovery load must skip once the instance is loaded", analyzer.shouldReload(null));
            analyzer.reload(null, "my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter, noOp());
            assertSame("a recovery reload after the initial load must not rebuild", afterInitialLoad, analyzer.getComponents());

            // An explicit request (non-null token) always rebuilds.
            assertTrue("an explicit reload request must rebuild", analyzer.shouldReload(new ReloadToken()));
            analyzer.reload(new ReloadToken(), "my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter, noOp());
            assertNotSame("an explicit reload must rebuild", afterInitialLoad, analyzer.getComponents());
        }
    }

    private static Map<String, TokenFilterFactory> noOp() {
        return Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER);
    }

    /**
     * One {@code _reload_search_analyzers} request fans out to every shard, and the indices sharing one
     * {@link ReloadableCustomAnalyzer} on a node carry the same request token. The first sharer attempts
     * the rebuild; a later sharer carrying the same token must observe that outcome rather than rebuild.
     * Crucially, a failed attempt is remembered and <em>replayed</em> to same-token sharers — their shard
     * reports the same failure without re-running the build. Sharers carry a byte-identical recipe, so a
     * rebuild would only repeat the failure; for a resource failure such as a synonym map tripping the
     * circuit breaker, retrying on every sharing shard would turn one failure into a storm. Only a fresh
     * request (a new token) rebuilds.
     */
    public void testFailedReloadIsReplayedToSameRequestSharers() throws IOException {
        AtomicInteger rebuildAttempts = new AtomicInteger();
        TokenFilterFactory failsFirstReload = new AbstractTokenFilterFactory("my_filter") {
            @Override
            public AnalysisMode getAnalysisMode() {
                return AnalysisMode.SEARCH_TIME;
            }

            @Override
            public TokenFilterFactory getChainAwareTokenFilterFactory(
                IndexCreationContext context,
                TokenizerFactory tokenizer,
                List<CharFilterFactory> charFilters,
                List<TokenFilterFactory> previousTokenFilters,
                Function<String, TokenFilterFactory> allFilters
            ) {
                // Attempt 1 is the initial build (must succeed); attempt 2 is the first explicit reload,
                // which we fail to exercise the replay path; the next rebuild (a fresh request) succeeds.
                if (rebuildAttempts.incrementAndGet() == 2) {
                    throw new IllegalStateException("simulated reload failure");
                }
                return this;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };

        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        Map<String, TokenFilterFactory> tokenFilters = Collections.singletonMap("my_filter", failsFirstReload);
        AnalyzerComponents initial = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            tokenFilters
        );
        assertEquals("the initial build is the first rebuild attempt", 1, rebuildAttempts.get());

        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(initial, 0, 0)) {
            ReloadToken failedToken = new ReloadToken();
            IllegalStateException failure = expectThrows(
                IllegalStateException.class,
                () -> analyzer.reload(
                    failedToken,
                    "my_analyzer",
                    analyzerSettings,
                    testAnalysis.tokenizer,
                    testAnalysis.charFilter,
                    tokenFilters
                )
            );
            assertEquals("simulated reload failure", failure.getMessage());
            assertEquals("the first sharer attempted the rebuild", 2, rebuildAttempts.get());

            // A second sharer carrying the SAME token must replay the remembered failure, not rebuild.
            assertTrue("a same-token sharer must re-enter reload() to replay the failure", analyzer.shouldReload(failedToken));
            IllegalStateException replayed = expectThrows(
                IllegalStateException.class,
                () -> analyzer.reload(
                    failedToken,
                    "my_analyzer",
                    analyzerSettings,
                    testAnalysis.tokenizer,
                    testAnalysis.charFilter,
                    tokenFilters
                )
            );
            assertSame("the same failure instance must be replayed, not a fresh build's", failure, replayed);
            assertEquals("a same-token sharer must NOT rebuild after a failure (no circuit-breaker storm)", 2, rebuildAttempts.get());

            // A fresh request (new token) rebuilds; this attempt succeeds and clears the remembered failure.
            ReloadToken freshToken = new ReloadToken();
            assertTrue("a new request must rebuild", analyzer.shouldReload(freshToken));
            analyzer.reload(freshToken, "my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter, tokenFilters);
            assertEquals("a fresh request rebuilds", 3, rebuildAttempts.get());

            // After success the token dedups: a same-token sharer coasts on the fresh state, no rebuild.
            assertFalse("a same-token sharer after success must coast", analyzer.shouldReload(freshToken));
            analyzer.reload(freshToken, "my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter, tokenFilters);
            assertEquals("a successful same-token reload must coast, not rebuild", 3, rebuildAttempts.get());
        }
    }

    /**
     * Once the last sharer releases a shared {@link ReloadableCustomAnalyzer} the registry closes it.
     * A reload that was already in flight for that instance must quietly discard its result rather
     * than swap new components into — and keep alive — a torn-down analyzer. {@code close()} wins and
     * does not wait for the rebuild.
     */
    public void testReloadAfterCloseIsDiscarded() throws IOException {
        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents initial = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );

        ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(initial, 0, 0);
        AnalyzerComponents before = analyzer.getComponents();
        analyzer.close();

        // A reload arriving after close() must be a no-op (the components stay as they were) and must
        // not throw, and a closed instance must report that no reload is needed.
        analyzer.reload(
            new ReloadToken(),
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", LOWERCASE_SEARCH_TIME_FILTER)
        );
        assertSame("a reload after close must not swap in new components", before, analyzer.getComponents());
        assertFalse("a closed instance must report no reload is needed", analyzer.shouldReload(new ReloadToken()));
    }

    /**
     * The mid-build close re-check: if close() lands while a reload is already building (parked in the
     * heavy createComponents), reload must drop its freshly built components rather than publish them onto
     * the torn-down instance — and close() must not block waiting for the build. A probe parks the
     * builder; the test closes the analyzer while it is parked, releases it, and asserts the components
     * were not swapped in.
     */
    public void testReloadDiscardsResultWhenClosedMidBuild() throws Exception {
        CountDownLatch buildEntered = new CountDownLatch(1);
        CountDownLatch releaseBuild = new CountDownLatch(1);
        TokenFilterFactory parkingProbe = new AbstractTokenFilterFactory("my_filter") {
            @Override
            public AnalysisMode getAnalysisMode() {
                return AnalysisMode.SEARCH_TIME;
            }

            @Override
            public TokenFilterFactory getChainAwareTokenFilterFactory(
                IndexCreationContext context,
                TokenizerFactory tokenizer,
                List<CharFilterFactory> charFilters,
                List<TokenFilterFactory> previousTokenFilters,
                Function<String, TokenFilterFactory> allFilters
            ) {
                buildEntered.countDown();
                try {
                    releaseBuild.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return this;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };

        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents initial = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            noOp()
        );
        ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(initial, 0, 0);
        AnalyzerComponents before = analyzer.getComponents();
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread builder = new Thread(() -> {
            try {
                analyzer.reload(
                    new ReloadToken(),
                    "my_analyzer",
                    analyzerSettings,
                    testAnalysis.tokenizer,
                    testAnalysis.charFilter,
                    Collections.singletonMap("my_filter", parkingProbe)
                );
            } catch (Exception e) {
                failure.compareAndSet(null, e);
            }
        });
        builder.start();
        assertTrue("builder did not enter the build", buildEntered.await(10, TimeUnit.SECONDS));
        // close() while the reload is parked mid-build. It must not block (it is not synchronized).
        analyzer.close();
        releaseBuild.countDown();
        builder.join(30_000);

        assertFalse("builder thread did not finish", builder.isAlive());
        assertNull("reload threw: " + failure.get(), failure.get());
        assertSame("a reload that finishes building after close() must discard its result", before, analyzer.getComponents());
    }

    /**
     * Two concurrent {@link ReloadableCustomAnalyzer#reload} calls on the same instance must not build
     * the (potentially expensive) analyzer in parallel; the rebuilds serialize so only one runs at a
     * time. The probe filter holds the first builder inside the build (no timing — a latch), then the
     * test asserts a second reload {@code BLOCKED}s on the reload lock rather than entering the build.
     * Without serialization the second builder would enter ({@code inBuild == 2}) and the test fails.
     */
    public void testConcurrentReloadsDoNotBuildInParallel() throws Exception {
        AtomicInteger inBuild = new AtomicInteger();
        AtomicInteger peakConcurrentBuilds = new AtomicInteger();
        CountDownLatch firstBuilderEntered = new CountDownLatch(1);
        CountDownLatch releaseBuilders = new CountDownLatch(1);
        TokenFilterFactory serializationProbe = new AbstractTokenFilterFactory("my_filter") {
            @Override
            public AnalysisMode getAnalysisMode() {
                return AnalysisMode.SEARCH_TIME;
            }

            @Override
            public TokenFilterFactory getChainAwareTokenFilterFactory(
                IndexCreationContext context,
                TokenizerFactory tokenizer,
                List<CharFilterFactory> charFilters,
                List<TokenFilterFactory> previousTokenFilters,
                Function<String, TokenFilterFactory> allFilters
            ) {
                peakConcurrentBuilds.accumulateAndGet(inBuild.incrementAndGet(), Math::max);
                try {
                    firstBuilderEntered.countDown();
                    releaseBuilders.await(); // hold the builder here until the test releases it
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    inBuild.decrementAndGet();
                }
                return this;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };

        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        // Build the initial components with the no-op filter so this setup call does not block on the
        // probe; the probe (which holds its builder) is only used by the reload() calls below.
        AnalyzerComponents initial = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );

        AtomicReference<Exception> failure = new AtomicReference<>();
        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(initial, 0, 0)) {
            Runnable reload = () -> {
                try {
                    // Distinct token per call so both reloads are genuine rebuilds (no dedup), exercising
                    // serialization.
                    analyzer.reload(
                        new ReloadToken(),
                        "my_analyzer",
                        analyzerSettings,
                        testAnalysis.tokenizer,
                        testAnalysis.charFilter,
                        Collections.singletonMap("my_filter", serializationProbe)
                    );
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
            };
            Thread first = new Thread(reload);
            Thread second = new Thread(reload);
            first.start();
            // Wait until the first reload is inside the build, holding the reload lock.
            assertTrue("first builder did not enter", firstBuilderEntered.await(10, TimeUnit.SECONDS));
            second.start();
            // The second reload must block trying to acquire the reload lock — it cannot enter the build
            // while the first holds it. (Without serialization it would enter and inBuild would reach 2.)
            assertBusy(() -> assertEquals(Thread.State.BLOCKED, second.getState()));
            assertEquals("only one builder may be inside the build at a time", 1, inBuild.get());
            releaseBuilders.countDown();
            first.join(30_000);
            second.join(30_000);
        }

        assertNull("concurrent reload threw: " + failure.get(), failure.get());
        assertEquals("reload builds must never run in parallel for one instance", 1, peakConcurrentBuilds.get());
    }

    /**
     * A {@code tokenStream()} that races the close of a shared {@link ReloadableCustomAnalyzer} (the
     * last sharer was deleted) must fail like any other closed Lucene analyzer — an
     * {@link AlreadyClosedException} — rather than a raw NPE from the torn-down
     * {@link org.apache.lucene.util.CloseableThreadLocal}.
     */
    public void testTokenStreamAfterCloseThrowsAlreadyClosed() throws IOException {
        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents components = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );

        ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(components, 0, 0);
        analyzer.close();
        expectThrows(AlreadyClosedException.class, () -> analyzer.tokenStream("f", "foo"));
    }

    /**
     * Hardening for the close()/tokenStream() race: a query tokenizing through a shared analyzer while it
     * is closed (its last sharer released) must never observe a raw {@link NullPointerException} from the
     * torn-down {@link org.apache.lucene.util.CloseableThreadLocal} — only a graceful
     * {@link AlreadyClosedException}, or success if it got in first. A reader thread opens streams in a
     * tight loop while the main thread closes the analyzer mid-flight; any non-{@code AlreadyClosedException}
     * failure fails the test. (Either outcome — success or {@code AlreadyClosedException} — is acceptable,
     * so the assertion is not timing-dependent.)
     */
    public void testTokenStreamRacingCloseNeverNpes() throws Exception {
        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents components = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            noOp()
        );

        ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(components, 0, 0);
        AtomicReference<Throwable> unexpected = new AtomicReference<>();
        CountDownLatch tokenizedAtLeastOnce = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            while (true) {
                try (TokenStream ts = analyzer.tokenStream("f", "foo bar baz")) {
                    ts.reset();
                    while (ts.incrementToken()) {
                    }
                    ts.end();
                    tokenizedAtLeastOnce.countDown();
                } catch (AlreadyClosedException expected) {
                    return; // analyzer closed — the graceful, expected outcome
                } catch (Throwable t) {
                    unexpected.compareAndSet(null, t);
                    return;
                }
            }
        });
        reader.start();
        assertTrue("reader did not tokenize before close", tokenizedAtLeastOnce.await(10, TimeUnit.SECONDS));
        analyzer.close();
        reader.join(30_000);

        assertFalse("reader thread did not finish", reader.isAlive());
        assertNull("a tokenStream racing close must fail with AlreadyClosedException, never NPE: " + unexpected.get(), unexpected.get());
    }

    /**
     * An open {@link TokenStream} carries a concrete pipeline (Tokenizer + filter chain) built
     * from the {@link AnalyzerComponents} current at the time the stream was opened. A subsequent
     * {@link ReloadableCustomAnalyzer#reload} writes a new {@code AnalyzerComponents} to the
     * {@code volatile} field, but cannot reach the already-materialised pipeline objects.
     *
     * <p>The in-flight stream must therefore complete entirely with the OLD components; only a
     * freshly opened stream picks up the new ones. This guarantee comes from two properties:
     * <ol>
     *   <li>The {@code volatile} write is a single atomic pointer swap to a fully-built,
     *       immutable {@code AnalyzerComponents} — no partial state is ever visible.
     *   <li>The TokenStream pipeline ({@code Tokenizer} + each {@code TokenFilter}) is
     *       instantiated once at {@code createComponents()} time and holds no back-reference to
     *       the {@code volatile} field; its {@code incrementToken()} calls run entirely on those
     *       concrete objects.
     * </ol>
     *
     * <p>This test is deliberately single-threaded: the isolation guarantee is structural, not
     * contingent on scheduling or locking.
     */
    public void testInFlightTokenStreamIsIsolatedFromSubsequentReload() throws IOException {
        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents components = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );

        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(components, 0, 0)) {
            // Open a token stream under the original (no-op) components — input stays upper-case.
            TokenStream stream = analyzer.tokenStream("f", "FOO BAR BAZ");
            stream.reset();
            CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);

            // Consume the first token: no-op filter preserves upper-case.
            assertTrue(stream.incrementToken());
            assertEquals("FOO", term.toString());

            // Reload with the lowercase filter while the stream is still open and mid-sequence.
            // The volatile write is immediately visible to any thread reading components, but the
            // pipeline that was materialised at stream-creation time (Tokenizer + TokenFilter chain)
            // has no back-pointer to the volatile field.
            analyzer.reload(
                new ReloadToken(),
                "my_analyzer",
                analyzerSettings,
                testAnalysis.tokenizer,
                testAnalysis.charFilter,
                Collections.singletonMap("my_filter", LOWERCASE_SEARCH_TIME_FILTER)
            );

            // The remaining tokens in the SAME stream must still be upper-case (old pipeline intact).
            assertTrue(stream.incrementToken());
            assertEquals("BAR", term.toString());
            assertTrue(stream.incrementToken());
            assertEquals("BAZ", term.toString());
            assertFalse(stream.incrementToken());
            stream.end();
            stream.close();

            // A freshly opened stream must now reflect the new (lowercase) components.
            try (TokenStream fresh = analyzer.tokenStream("f", "FOO BAR")) {
                fresh.reset();
                CharTermAttribute freshTerm = fresh.addAttribute(CharTermAttribute.class);
                assertTrue(fresh.incrementToken());
                assertEquals("foo", freshTerm.toString());
                fresh.end();
            }
        }
    }

    /**
     * {@link ReloadableCustomAnalyzer#reload} is {@code synchronized}: when N threads call it
     * concurrently they serialise and each builds a complete, self-consistent
     * {@link AnalyzerComponents}. The last writer wins (single volatile pointer swap), but no
     * caller can ever observe a partially-constructed component set.
     */
    public void testConcurrentReloadsProduceConsistentState() throws Exception {
        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents initial = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );

        int nThreads = randomIntBetween(4, 8);
        CountDownLatch ready = new CountDownLatch(nThreads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(initial, 0, 0)) {
            Thread[] threads = new Thread[nThreads];
            for (int i = 0; i < nThreads; i++) {
                threads[i] = new Thread(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        analyzer.reload(
                            new ReloadToken(),
                            "my_analyzer",
                            analyzerSettings,
                            testAnalysis.tokenizer,
                            testAnalysis.charFilter,
                            Collections.singletonMap("my_filter", LOWERCASE_SEARCH_TIME_FILTER)
                        );
                    } catch (Exception e) {
                        failure.compareAndSet(null, e);
                    }
                });
            }
            for (Thread t : threads) {
                t.start();
            }
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            go.countDown();
            for (Thread t : threads) {
                t.join(30_000);
            }

            assertNull("concurrent reload threw: " + failure.get(), failure.get());

            // The volatile pointer is always swapped to a fully-built immutable object, so the
            // final state must be consistent — never a mix of old and new components.
            try (TokenStream ts = analyzer.tokenStream("f", "FOO")) {
                ts.reset();
                CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
                assertTrue(ts.incrementToken());
                assertEquals("foo", term.toString());
                ts.end();
            }
        }
    }

    /**
     * The once-per-request dedup lives in {@link ReloadableCustomAnalyzer#reload}: when N threads
     * concurrently reload with the same token — a broadcast of one request reaching several shards that
     * share the instance — exactly one rebuild happens and the rest dedup on the token. A fresh token
     * (the next request) rebuilds again. A token filter counts how many times the components are built.
     */
    public void testConcurrentReloadWithSameTokenRebuildsOnce() throws Exception {
        AtomicInteger builds = new AtomicInteger();
        TokenFilterFactory buildCounter = new AbstractTokenFilterFactory("my_filter") {
            @Override
            public AnalysisMode getAnalysisMode() {
                return AnalysisMode.SEARCH_TIME;
            }

            @Override
            public TokenFilterFactory getChainAwareTokenFilterFactory(
                IndexCreationContext context,
                TokenizerFactory tokenizer,
                List<CharFilterFactory> charFilters,
                List<TokenFilterFactory> previousTokenFilters,
                Function<String, TokenFilterFactory> allFilters
            ) {
                builds.incrementAndGet();
                return this;
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }

            @Override
            public Object sharingKey() {
                return this;
            }
        };

        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();
        AnalyzerComponents initial = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            noOp()
        );

        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(initial, 0, 0)) {
            int nThreads = randomIntBetween(4, 12);
            for (int round = 1; round <= 3; round++) {
                ReloadToken token = new ReloadToken();
                int buildsBefore = builds.get();
                CountDownLatch ready = new CountDownLatch(nThreads);
                CountDownLatch go = new CountDownLatch(1);
                AtomicReference<Exception> failure = new AtomicReference<>();

                Thread[] threads = new Thread[nThreads];
                for (int i = 0; i < nThreads; i++) {
                    threads[i] = new Thread(() -> {
                        ready.countDown();
                        try {
                            go.await();
                            analyzer.reload(
                                token,
                                "my_analyzer",
                                analyzerSettings,
                                testAnalysis.tokenizer,
                                testAnalysis.charFilter,
                                Collections.singletonMap("my_filter", buildCounter)
                            );
                        } catch (Exception e) {
                            failure.compareAndSet(null, e);
                        }
                    });
                }
                for (Thread t : threads) {
                    t.start();
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS));
                go.countDown();
                for (Thread t : threads) {
                    t.join(30_000);
                }

                assertNull("concurrent reload threw: " + failure.get(), failure.get());
                assertEquals("round " + round + ": one shared request token must rebuild exactly once", buildsBefore + 1, builds.get());
            }
        }
    }

    /**
     * start multiple threads that create token streams from this analyzer until reloaded tokenfilter takes effect
     */
    public void testReloading() throws IOException, InterruptedException {
        Settings analyzerSettings = Settings.builder().put("tokenizer", "standard").putList("filter", "my_filter").build();

        AnalyzerComponents components = createComponents(
            IndexCreationContext.RELOAD_ANALYZERS,
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER)
        );
        int numThreads = randomIntBetween(5, 10);

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CountDownLatch firstCheckpoint = new CountDownLatch(numThreads);
        CountDownLatch secondCheckpoint = new CountDownLatch(numThreads);

        try (ReloadableCustomAnalyzer analyzer = new ReloadableCustomAnalyzer(components, 0, 0)) {
            executorService.submit(() -> {
                while (secondCheckpoint.getCount() > 0) {
                    try (TokenStream firstTokenStream = analyzer.tokenStream("myField", "TEXT")) {
                        firstTokenStream.reset();
                        CharTermAttribute term = firstTokenStream.addAttribute(CharTermAttribute.class);
                        assertTrue(firstTokenStream.incrementToken());
                        if (term.toString().equals("TEXT")) {
                            firstCheckpoint.countDown();
                        }
                        if (term.toString().equals("text")) {
                            secondCheckpoint.countDown();
                        }
                        assertFalse(firstTokenStream.incrementToken());
                        firstTokenStream.end();
                    } catch (Exception e) {
                        throw ExceptionsHelper.convertToRuntime(e);
                    }
                }
            });

            // wait until all running threads have seen the unaltered upper case analysis at least once
            assertTrue(firstCheckpoint.await(5, TimeUnit.SECONDS));

            analyzer.reload(
                new ReloadToken(),
                "my_analyzer",
                analyzerSettings,
                testAnalysis.tokenizer,
                testAnalysis.charFilter,
                Collections.singletonMap("my_filter", LOWERCASE_SEARCH_TIME_FILTER)
            );

            // wait until all running threads have seen the new lower case analysis at least once
            assertTrue(secondCheckpoint.await(5, TimeUnit.SECONDS));

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
