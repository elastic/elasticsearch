/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.analysis.common;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.HeaderWarningAppender;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalysisTestsHelper;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertAnalyzesTo;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.instanceOf;

public class StemmerTokenFilterFactoryTests extends ESTokenStreamTestCase {

    private ThreadContext threadContext;
    private HeaderWarningAppender headerWarningAppender;

    @Before
    public final void before() {
        this.threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        this.headerWarningAppender = HeaderWarningAppender.createAppender("header_warning", null);
        this.headerWarningAppender.start();
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.headerWarningAppender);
    }

    @After
    public final void after() {
        HeaderWarning.removeThreadContext(threadContext);
        threadContext = null;
        if (this.headerWarningAppender != null) {
            Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.headerWarningAppender);
            this.headerWarningAppender = null;
        }
    }

    private static final CommonAnalysisPlugin PLUGIN = new CommonAnalysisPlugin();

    public void testEnglishFilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {
            IndexVersion v = IndexVersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                .put("index.analysis.filter.my_english.type", "stemmer")
                .put("index.analysis.filter.my_english.language", "english")
                .put("index.analysis.analyzer.my_english.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_english.filter", "my_english")
                .put(SETTING_VERSION_CREATED, v)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();

            ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_english");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_english");
            assertThat(create, instanceOf(PorterStemFilter.class));
            assertAnalyzesTo(analyzer, "consolingly", new String[] { "consolingli" });
        }
    }

    public void testPorter2FilterFactory() throws IOException {
        int iters = scaledRandomIntBetween(20, 100);
        for (int i = 0; i < iters; i++) {

            IndexVersion v = IndexVersionUtils.randomVersion(random());
            Settings settings = Settings.builder()
                .put("index.analysis.filter.my_porter2.type", "stemmer")
                .put("index.analysis.filter.my_porter2.language", "porter2")
                .put("index.analysis.analyzer.my_porter2.tokenizer", "whitespace")
                .put("index.analysis.analyzer.my_porter2.filter", "my_porter2")
                .put(SETTING_VERSION_CREATED, v)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();

            ESTestCase.TestAnalysis analysis = AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            TokenFilterFactory tokenFilter = analysis.tokenFilter.get("my_porter2");
            assertThat(tokenFilter, instanceOf(StemmerTokenFilterFactory.class));
            Tokenizer tokenizer = new WhitespaceTokenizer();
            tokenizer.setReader(new StringReader("foo bar"));
            TokenStream create = tokenFilter.create(tokenizer);
            IndexAnalyzers indexAnalyzers = analysis.indexAnalyzers;
            NamedAnalyzer analyzer = indexAnalyzers.get("my_porter2");
            assertThat(create, instanceOf(SnowballFilter.class));
            assertAnalyzesTo(analyzer, "possibly", new String[] { "possibl" });
        }
    }

    public void testMultipleLanguagesThrowsException() throws IOException {
        IndexVersion v = IndexVersionUtils.randomVersion(random());
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_english.type", "stemmer")
            .putList("index.analysis.filter.my_english.language", "english", "light_english")
            .put(SETTING_VERSION_CREATED, v)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN)
        );
        assertEquals("Invalid stemmer class specified: [english, light_english]", e.getMessage());
    }

    public void testKpDeprecation() throws IOException {
        IndexVersion v = IndexVersionUtils.randomVersion(random());
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_kp.type", "stemmer")
            .put("index.analysis.filter.my_kp.language", "kp")
            .put(SETTING_VERSION_CREATED, v)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
        try {
            final List<String> actualWarningStrings = threadContext.getResponseHeaders().get("Warning");
            assertTrue(actualWarningStrings.stream().anyMatch(warning -> warning.contains("The [dutch_kp] stemmer is deprecated")));
        } finally {
            threadContext.stashContext();
        }
    }

    public void testLovinsDeprecation() throws IOException {
        IndexVersion v = IndexVersionUtils.randomVersion(random());
        Settings settings = Settings.builder()
            .put("index.analysis.filter.my_lovins.type", "stemmer")
            .put("index.analysis.filter.my_lovins.language", "lovins")
            .put(SETTING_VERSION_CREATED, v)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();

        try {
            AnalysisTestsHelper.createTestAnalysisFromSettings(settings, PLUGIN);
            final List<String> actualWarningStrings = threadContext.getResponseHeaders().get("Warning");
            assertTrue(actualWarningStrings.stream().anyMatch(warning -> warning.contains("The [lovins] stemmer is deprecated")));
        } finally {
            threadContext.stashContext();
        }
    }
}
