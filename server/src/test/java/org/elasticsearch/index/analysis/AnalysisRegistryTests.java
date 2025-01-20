/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.MockTokenFilter;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AnalysisRegistryTests extends ESTestCase {
    private AnalysisRegistry emptyRegistry;
    private AnalysisRegistry nonEmptyRegistry;

    private static AnalyzerProvider<?> analyzerProvider(final String name) {
        return new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDEX, new EnglishAnalyzer());
    }

    private static AnalysisRegistry emptyAnalysisRegistry(Settings settings) {
        return new AnalysisRegistry(
            TestEnvironment.newEnvironment(settings),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
    }

    /**
     * Creates a reverse filter available for use in testNameClashNormalizer test
     */
    public static class MockAnalysisPlugin extends Plugin implements AnalysisPlugin {
        @Override
        public List<PreConfiguredTokenFilter> getPreConfiguredTokenFilters() {
            return singletonList(PreConfiguredTokenFilter.singleton("reverse", true, ReverseStringFilter::new));
        }
    }

    private static IndexSettings indexSettingsOfCurrentVersion(Settings.Builder settings) {
        return IndexSettingsModule.newIndexSettings(
            "index",
            settings.put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        emptyRegistry = emptyAnalysisRegistry(settings);
        // Module loaded to register in-built normalizers for testing
        AnalysisModule module = new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            singletonList(new MockAnalysisPlugin()),
            new StablePluginsRegistry()
        );
        nonEmptyRegistry = module.getAnalysisRegistry();
    }

    public void testDefaultAnalyzers() throws IOException {
        IndexVersion version = IndexVersionUtils.randomVersion();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        IndexAnalyzers indexAnalyzers = emptyRegistry.build(IndexCreationContext.CREATE_INDEX, idxSettings);
        assertThat(indexAnalyzers.getDefaultIndexAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchQuoteAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(indexAnalyzers.get(AnalysisRegistry.DEFAULT_ANALYZER_NAME).name(), equalTo("default"));
    }

    public void testOverrideDefaultAnalyzer() throws IOException {
        IndexVersion version = IndexVersionUtils.randomVersion();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        IndexAnalyzers indexAnalyzers = AnalysisRegistry.build(
            IndexCreationContext.CREATE_INDEX,
            IndexSettingsModule.newIndexSettings("index", settings),
            singletonMap("default", analyzerProvider("default")),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        assertThat(indexAnalyzers.getDefaultIndexAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchQuoteAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
    }

    public void testOverrideDefaultAnalyzerWithoutAnalysisModeAll() throws IOException {
        IndexVersion version = IndexVersionUtils.randomVersion();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("index", settings);
        TokenFilterFactory tokenFilter = new AbstractTokenFilterFactory("my_filter") {
            @Override
            public AnalysisMode getAnalysisMode() {
                return randomFrom(AnalysisMode.SEARCH_TIME, AnalysisMode.INDEX_TIME);
            }

            @Override
            public TokenStream create(TokenStream tokenStream) {
                return tokenStream;
            }
        };
        TokenizerFactory tokenizer = new AbstractTokenizerFactory("my_tokenizer") {
            @Override
            public Tokenizer create() {
                return new StandardTokenizer();
            }
        };
        Analyzer analyzer = new CustomAnalyzer(tokenizer, new CharFilterFactory[0], new TokenFilterFactory[] { tokenFilter });
        MapperException ex = expectThrows(
            MapperException.class,
            () -> AnalysisRegistry.build(
                IndexCreationContext.CREATE_INDEX,
                IndexSettingsModule.newIndexSettings("index", settings),
                singletonMap("default", new PreBuiltAnalyzerProvider("default", AnalyzerScope.INDEX, analyzer)),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap()
            )
        );
        assertEquals("analyzer [default] contains filters [my_filter] that are not allowed to run in all mode.", ex.getMessage());
    }

    public void testNameClashNormalizer() throws IOException {

        // Test out-of-the-box normalizer works OK.
        IndexAnalyzers indexAnalyzers = nonEmptyRegistry.build(
            IndexCreationContext.CREATE_INDEX,
            IndexSettingsModule.newIndexSettings("index", Settings.EMPTY)
        );
        assertNotNull(indexAnalyzers.getNormalizer("lowercase"));
        assertThat(indexAnalyzers.getNormalizer("lowercase").normalize("field", "AbC").utf8ToString(), equalTo("abc"));

        // Test that a name clash with a custom normalizer will favour the index's normalizer rather than the out-of-the-box
        // one of the same name. (However this "feature" will be removed with https://github.com/elastic/elasticsearch/issues/22263 )
        Settings settings = Settings.builder()
            // Deliberately bad choice of normalizer name for the job it does.
            .put("index.analysis.normalizer.lowercase.type", "custom")
            .putList("index.analysis.normalizer.lowercase.filter", "reverse")
            .build();

        indexAnalyzers = nonEmptyRegistry.build(IndexCreationContext.CREATE_INDEX, IndexSettingsModule.newIndexSettings("index", settings));
        assertNotNull(indexAnalyzers.getNormalizer("lowercase"));
        assertThat(indexAnalyzers.getNormalizer("lowercase").normalize("field", "AbC").utf8ToString(), equalTo("CbA"));
    }

    public void testOverrideDefaultIndexAnalyzerIsUnsupported() {
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        AnalyzerProvider<?> defaultIndex = new PreBuiltAnalyzerProvider("default_index", AnalyzerScope.INDEX, new EnglishAnalyzer());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> AnalysisRegistry.build(
                IndexCreationContext.CREATE_INDEX,
                IndexSettingsModule.newIndexSettings("index", settings),
                singletonMap("default_index", defaultIndex),
                emptyMap(),
                emptyMap(),
                emptyMap(),
                emptyMap()
            )
        );
        assertTrue(e.getMessage().contains("[index.analysis.analyzer.default_index] is not supported"));
    }

    public void testOverrideDefaultSearchAnalyzer() {
        IndexVersion version = IndexVersionUtils.randomVersion();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        IndexAnalyzers indexAnalyzers = AnalysisRegistry.build(
            IndexCreationContext.CREATE_INDEX,
            IndexSettingsModule.newIndexSettings("index", settings),
            singletonMap("default_search", analyzerProvider("default_search")),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()
        );
        assertThat(indexAnalyzers.getDefaultIndexAnalyzer().analyzer(), instanceOf(StandardAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
        assertThat(indexAnalyzers.getDefaultSearchQuoteAnalyzer().analyzer(), instanceOf(EnglishAnalyzer.class));
    }

    /**
     * Tests that {@code camelCase} filter names and {@code snake_case} filter names don't collide.
     */
    public void testConfigureCamelCaseTokenFilter() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.analysis.filter.testFilter.type", "mock")
            .put("index.analysis.filter.test_filter.type", "mock")
            .put("index.analysis.analyzer.custom_analyzer_with_camel_case.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer_with_camel_case.filter", "lowercase", "testFilter")
            .put("index.analysis.analyzer.custom_analyzer_with_snake_case.tokenizer", "standard")
            .putList("index.analysis.analyzer.custom_analyzer_with_snake_case.filter", "lowercase", "test_filter")
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        /* The snake_case version of the name should not filter out any stopwords while the
         * camelCase version will filter out English stopwords. */
        AnalysisPlugin plugin = new AnalysisPlugin() {
            class MockFactory extends AbstractTokenFilterFactory {
                MockFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
                    super(name);
                }

                @Override
                public TokenStream create(TokenStream tokenStream) {
                    if (name().equals("test_filter")) {
                        return new MockTokenFilter(tokenStream, MockTokenFilter.EMPTY_STOPSET);
                    }
                    return new MockTokenFilter(tokenStream, MockTokenFilter.ENGLISH_STOPSET);
                }
            }

            @Override
            public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                return singletonMap("mock", MockFactory::new);
            }
        };
        IndexAnalyzers indexAnalyzers = new AnalysisModule(
            TestEnvironment.newEnvironment(settings),
            singletonList(plugin),
            new StablePluginsRegistry()
        ).getAnalysisRegistry().build(IndexCreationContext.CREATE_INDEX, idxSettings);

        // This shouldn't contain English stopwords
        try (NamedAnalyzer custom_analyser = indexAnalyzers.get("custom_analyzer_with_camel_case")) {
            assertNotNull(custom_analyser);
            TokenStream tokenStream = custom_analyser.tokenStream("foo", "has a foo");
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            assertTrue(tokenStream.incrementToken());
            assertEquals("has", charTermAttribute.toString());
            assertTrue(tokenStream.incrementToken());
            assertEquals("foo", charTermAttribute.toString());
            assertFalse(tokenStream.incrementToken());
        }

        // This *should* contain English stopwords
        try (NamedAnalyzer custom_analyser = indexAnalyzers.get("custom_analyzer_with_snake_case")) {
            assertNotNull(custom_analyser);
            TokenStream tokenStream = custom_analyser.tokenStream("foo", "has a foo");
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            assertTrue(tokenStream.incrementToken());
            assertEquals("has", charTermAttribute.toString());
            assertTrue(tokenStream.incrementToken());
            assertEquals("a", charTermAttribute.toString());
            assertTrue(tokenStream.incrementToken());
            assertEquals("foo", charTermAttribute.toString());
            assertFalse(tokenStream.incrementToken());
        }
    }

    public void testBuiltInAnalyzersAreCached() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);
        IndexAnalyzers indexAnalyzers = emptyAnalysisRegistry(settings).build(IndexCreationContext.CREATE_INDEX, idxSettings);
        IndexAnalyzers otherIndexAnalyzers = emptyAnalysisRegistry(settings).build(IndexCreationContext.CREATE_INDEX, idxSettings);
        final int numIters = randomIntBetween(5, 20);
        for (int i = 0; i < numIters; i++) {
            PreBuiltAnalyzers preBuiltAnalyzers = RandomPicks.randomFrom(random(), PreBuiltAnalyzers.values());
            assertSame(indexAnalyzers.get(preBuiltAnalyzers.name()), otherIndexAnalyzers.get(preBuiltAnalyzers.name()));
        }
    }

    public void testNoTypeOrTokenizerErrorMessage() {
        IndexVersion version = IndexVersionUtils.randomVersion();
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, version)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .putList("index.analysis.analyzer.test_analyzer.filter", new String[] { "lowercase", "stop", "shingle" })
            .putList("index.analysis.analyzer.test_analyzer.char_filter", new String[] { "html_strip" })
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> emptyAnalysisRegistry(settings).build(IndexCreationContext.CREATE_INDEX, idxSettings)
        );
        assertThat(e.getMessage(), equalTo("analyzer [test_analyzer] must specify either an analyzer type, or a tokenizer"));
    }

    public void testCloseIndexAnalyzersMultipleTimes() throws IOException {
        IndexAnalyzers indexAnalyzers = emptyRegistry.build(
            IndexCreationContext.CREATE_INDEX,
            indexSettingsOfCurrentVersion(Settings.builder())
        );
        indexAnalyzers.close();
        indexAnalyzers.close();
    }

    public void testEnsureCloseInvocationProperlyDelegated() throws IOException {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        PreBuiltAnalyzerProviderFactory mock = mock(PreBuiltAnalyzerProviderFactory.class);
        AnalysisRegistry registry = new AnalysisRegistry(
            TestEnvironment.newEnvironment(settings),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            Collections.singletonMap("key", mock)
        );

        registry.close();
        verify(mock).close();
    }
}
