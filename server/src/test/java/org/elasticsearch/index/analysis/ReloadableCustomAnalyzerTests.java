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
        // not throw, and a closed instance must refuse to claim a reload token.
        analyzer.reload(
            "my_analyzer",
            analyzerSettings,
            testAnalysis.tokenizer,
            testAnalysis.charFilter,
            Collections.singletonMap("my_filter", LOWERCASE_SEARCH_TIME_FILTER)
        );
        assertSame("a reload after close must not swap in new components", before, analyzer.getComponents());
        assertFalse("a closed instance must refuse to claim a reload token", analyzer.tryClaimReload(new Object()));
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
                    analyzer.reload(
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
            for (Thread t : threads)
                t.start();
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            go.countDown();
            for (Thread t : threads)
                t.join(30_000);

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
     * {@link ReloadableCustomAnalyzer#tryClaimReload} is {@code synchronized}: when N threads
     * race to claim the same reload token, exactly one must receive {@code true} and the rest
     * {@code false}. A fresh token resets the gate and again allows exactly one claim.
     */
    public void testTryClaimReloadAllowsExactlyOneClaimPerToken() throws Exception {
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
            int nThreads = randomIntBetween(4, 12);

            for (int round = 0; round < 3; round++) {
                Object token = new Object();
                CountDownLatch ready = new CountDownLatch(nThreads);
                CountDownLatch go = new CountDownLatch(1);
                AtomicInteger claimCount = new AtomicInteger();

                Thread[] threads = new Thread[nThreads];
                for (int i = 0; i < nThreads; i++) {
                    threads[i] = new Thread(() -> {
                        ready.countDown();
                        try {
                            go.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        if (analyzer.tryClaimReload(token)) {
                            claimCount.incrementAndGet();
                        }
                    });
                }
                for (Thread t : threads)
                    t.start();
                assertTrue(ready.await(10, TimeUnit.SECONDS));
                go.countDown();
                for (Thread t : threads)
                    t.join(30_000);

                assertEquals("round " + round + ": exactly one thread must claim the token", 1, claimCount.get());
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
