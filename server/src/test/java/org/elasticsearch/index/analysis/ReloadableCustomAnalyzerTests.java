/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.analysis.AnalyzerComponents.createComponents;

public class ReloadableCustomAnalyzerTests extends ESTestCase {

    private static TestAnalysis testAnalysis;
    private static Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();

    private static TokenFilterFactory NO_OP_SEARCH_TIME_FILTER = new AbstractTokenFilterFactory(
            IndexSettingsModule.newIndexSettings("index", settings), "my_filter", Settings.EMPTY) {
        @Override
        public AnalysisMode getAnalysisMode() {
            return AnalysisMode.SEARCH_TIME;
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return tokenStream;
        }
    };

    private static TokenFilterFactory LOWERCASE_SEARCH_TIME_FILTER = new AbstractTokenFilterFactory(
            IndexSettingsModule.newIndexSettings("index", settings), "my_other_filter", Settings.EMPTY) {
        @Override
        public AnalysisMode getAnalysisMode() {
            return AnalysisMode.SEARCH_TIME;
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            return new LowerCaseFilter(tokenStream);
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

        Settings analyzerSettings = Settings.builder()
                .put("tokenizer", "standard")
                .putList("filter", "my_filter")
                .build();

        AnalyzerComponents components = createComponents("my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter,
                Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER));

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
        final Settings indexAnalyzerSettings = Settings.builder()
                .put("tokenizer", "standard")
                .putList("filter", "lowercase")
                .build();
        AnalyzerComponents indexAnalyzerComponents = createComponents("my_analyzer", indexAnalyzerSettings, testAnalysis.tokenizer,
                testAnalysis.charFilter, testAnalysis.tokenFilter);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> new ReloadableCustomAnalyzer(indexAnalyzerComponents, positionIncrementGap, offsetGap));
        assertEquals("ReloadableCustomAnalyzer must only be initialized with analysis components in AnalysisMode.SEARCH_TIME mode",
                ex.getMessage());
    }

    /**
     * start multiple threads that create token streams from this analyzer until reloaded tokenfilter takes effect
     */
    public void testReloading() throws IOException, InterruptedException {
        Settings analyzerSettings = Settings.builder()
                .put("tokenizer", "standard")
                .putList("filter", "my_filter")
                .build();

        AnalyzerComponents components = createComponents("my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter,
                Collections.singletonMap("my_filter", NO_OP_SEARCH_TIME_FILTER));
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

            analyzer.reload("my_analyzer", analyzerSettings, testAnalysis.tokenizer, testAnalysis.charFilter,
                    Collections.singletonMap("my_filter", LOWERCASE_SEARCH_TIME_FILTER));

            // wait until all running threads have seen the new lower case analysis at least once
            assertTrue(secondCheckpoint.await(5, TimeUnit.SECONDS));

            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.SECONDS);
        }
    }
}
