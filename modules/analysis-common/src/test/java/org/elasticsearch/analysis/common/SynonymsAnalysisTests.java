/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.tests.analysis.BaseTokenStreamTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.PreConfiguredTokenFilter;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

public class SynonymsAnalysisTests extends ESTestCase {
    private IndexAnalyzers indexAnalyzers;

    public void testSynonymsAnalysis() throws IOException {
        InputStream synonyms = getClass().getResourceAsStream("synonyms.txt");
        InputStream synonymsWordnet = getClass().getResourceAsStream("synonyms_wordnet.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(synonyms, config.resolve("synonyms.txt"));
        Files.copy(synonymsWordnet, config.resolve("synonyms_wordnet.txt"));

        String json = "/org/elasticsearch/analysis/common/synonyms.json";
        Settings settings = Settings.builder()
            .loadFromStream(json, getClass().getResourceAsStream(json), false)
            .put(Environment.PATH_HOME_SETTING.getKey(), home)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .build();

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        match("synonymAnalyzer", "kimchy is the dude abides", "shay is the elasticsearch man!");
        match("synonymAnalyzer_file", "kimchy is the dude abides", "shay is the elasticsearch man!");
        match("synonymAnalyzerWordnet", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWordnet_file", "abstain", "abstain refrain desist");
        match("synonymAnalyzerWithsettings", "kimchy", "sha hay");
        match("synonymAnalyzerWithStopAfterSynonym", "kimchy is the dude abides , stop", "shay is the elasticsearch man! ,");
        match("synonymAnalyzerWithStopBeforeSynonym", "kimchy is the dude abides , stop", "shay is the elasticsearch man! ,");
        match("synonymAnalyzerWithStopSynonymAfterSynonym", "kimchy is the dude abides", "shay is the man!");
        match("synonymAnalyzerExpand", "kimchy is the dude abides", "kimchy shay is the dude elasticsearch abides man!");
        match("synonymAnalyzerExpandWithStopAfterSynonym", "kimchy is the dude abides", "shay is the dude abides man!");

    }

    public void testSynonymWordDeleteByAnalyzer() throws IOException {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.my_synonym.type", "synonym")
            .putList("index.analysis.filter.my_synonym.synonyms", "kimchy => shay", "dude => elasticsearch", "abides => man!")
            .put("index.analysis.filter.stop_within_synonym.type", "stop")
            .putList("index.analysis.filter.stop_within_synonym.stopwords", "kimchy", "elasticsearch")
            .put("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.filter", "stop_within_synonym", "my_synonym");

        CheckedBiConsumer<IndexVersion, Boolean, IOException> assertIsLenient = (iv, updateable) -> {
            Settings settings = settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, iv)
                .put("index.analysis.filter.my_synonym.updateable", updateable)
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
            match("synonymAnalyzerWithStopSynonymBeforeSynonym", "kimchy is the dude abides", "is the dude man!");
        };

        BiConsumer<IndexVersion, Boolean> assertIsNotLenient = (iv, updateable) -> {
            Settings settings = settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, iv)
                .put("index.analysis.filter.my_synonym.updateable", updateable)
                .build();
            try {
                IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
                indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
                fail("fail! due to synonym word deleted by analyzer");
            } catch (Exception e) {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), startsWith("failed to build synonyms"));
                assertThat(e.getMessage(), containsString("['my_synonym' analyzer settings]"));
            }
        };

        // Test with an index version where lenient should always be false by default
        IndexVersion randomNonLenientIndexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersions.INDEX_SORTING_ON_NESTED
        );
        assertIsNotLenient.accept(randomNonLenientIndexVersion, false);
        assertIsNotLenient.accept(randomNonLenientIndexVersion, true);

        // Test with an index version where the default lenient value is based on updateable
        IndexVersion randomLenientIndexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.LENIENT_UPDATEABLE_SYNONYMS,
            IndexVersion.current()
        );
        assertIsNotLenient.accept(randomLenientIndexVersion, false);
        assertIsLenient.accept(randomLenientIndexVersion, true);
    }

    public void testSynonymWordDeleteByAnalyzerFromFile() throws IOException {
        InputStream synonyms = getClass().getResourceAsStream("synonyms.txt");
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(synonyms, config.resolve("synonyms.txt"));

        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", home)
            .put("index.analysis.filter.my_synonym.type", "synonym")
            .put("index.analysis.filter.my_synonym.synonyms_path", "synonyms.txt")
            .put("index.analysis.filter.stop_within_synonym.type", "stop")
            .putList("index.analysis.filter.stop_within_synonym.stopwords", "kimchy", "elasticsearch")
            .put("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.synonymAnalyzerWithStopSynonymBeforeSynonym.filter", "stop_within_synonym", "my_synonym");

        CheckedBiConsumer<IndexVersion, Boolean, IOException> assertIsLenient = (iv, updateable) -> {
            Settings settings = settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, iv)
                .put("index.analysis.filter.my_synonym.updateable", updateable)
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
            match("synonymAnalyzerWithStopSynonymBeforeSynonym", "kimchy is the dude abides", "is the dude man!");
        };

        BiConsumer<IndexVersion, Boolean> assertIsNotLenient = (iv, updateable) -> {
            Settings settings = settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, iv)
                .put("index.analysis.filter.my_synonym.updateable", updateable)
                .build();
            try {
                IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
                indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
                fail("fail! due to synonym word deleted by analyzer");
            } catch (Exception e) {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), equalTo("failed to build synonyms from [synonyms.txt]"));
            }
        };

        // Test with an index version where lenient should always be false by default
        IndexVersion randomNonLenientIndexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersions.INDEX_SORTING_ON_NESTED
        );
        assertIsNotLenient.accept(randomNonLenientIndexVersion, false);
        assertIsNotLenient.accept(randomNonLenientIndexVersion, true);

        // Test with an index version where the default lenient value is based on updateable
        IndexVersion randomLenientIndexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.LENIENT_UPDATEABLE_SYNONYMS,
            IndexVersion.current()
        );
        assertIsNotLenient.accept(randomLenientIndexVersion, false);
        assertIsLenient.accept(randomLenientIndexVersion, true);
    }

    public void testExpandSynonymWordDeleteByAnalyzer() throws IOException {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonym_expand.type", "synonym")
            .putList("index.analysis.filter.synonym_expand.synonyms", "kimchy, shay", "dude, elasticsearch", "abides, man!")
            .put("index.analysis.filter.stop_within_synonym.type", "stop")
            .putList("index.analysis.filter.stop_within_synonym.stopwords", "kimchy", "elasticsearch")
            .put("index.analysis.analyzer.synonymAnalyzerExpandWithStopBeforeSynonym.tokenizer", "whitespace")
            .putList("index.analysis.analyzer.synonymAnalyzerExpandWithStopBeforeSynonym.filter", "stop_within_synonym", "synonym_expand");

        CheckedBiConsumer<IndexVersion, Boolean, IOException> assertIsLenient = (iv, updateable) -> {
            Settings settings = settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, iv)
                .put("index.analysis.filter.synonym_expand.updateable", updateable)
                .build();
            IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
            match("synonymAnalyzerExpandWithStopBeforeSynonym", "kimchy is the dude abides", "is the dude abides man!");
        };

        BiConsumer<IndexVersion, Boolean> assertIsNotLenient = (iv, updateable) -> {
            Settings settings = settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, iv)
                .put("index.analysis.filter.synonym_expand.updateable", updateable)
                .build();
            try {
                IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
                indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
                fail("fail! due to synonym word deleted by analyzer");
            } catch (Exception e) {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), startsWith("failed to build synonyms"));
                assertThat(e.getMessage(), containsString("['synonym_expand' analyzer settings]"));
            }
        };

        // Test with an index version where lenient should always be false by default
        IndexVersion randomNonLenientIndexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.MINIMUM_READONLY_COMPATIBLE,
            IndexVersions.INDEX_SORTING_ON_NESTED
        );
        assertIsNotLenient.accept(randomNonLenientIndexVersion, false);
        assertIsNotLenient.accept(randomNonLenientIndexVersion, true);

        // Test with an index version where the default lenient value is based on updateable
        IndexVersion randomLenientIndexVersion = IndexVersionUtils.randomVersionBetween(
            random(),
            IndexVersions.LENIENT_UPDATEABLE_SYNONYMS,
            IndexVersion.current()
        );
        assertIsNotLenient.accept(randomLenientIndexVersion, false);
        assertIsLenient.accept(randomLenientIndexVersion, true);
    }

    public void testSynonymsWrappedByMultiplexer() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "programmer, developer")
            .put("index.analysis.filter.my_english.type", "stemmer")
            .put("index.analysis.filter.my_english.language", "porter2")
            .put("index.analysis.filter.stem_repeat.type", "multiplexer")
            .putList("index.analysis.filter.stem_repeat.filters", "my_english, synonyms")
            .put("index.analysis.analyzer.synonymAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonymAnalyzer.filter", "lowercase", "stem_repeat")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("synonymAnalyzer"),
            "Some developers are odd",
            new String[] { "some", "developers", "develop", "programm", "are", "odd" },
            new int[] { 1, 1, 0, 0, 1, 1 }
        );
    }

    public void testAsciiFoldingFilterForSynonyms() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "hoj, height")
            .put("index.analysis.analyzer.synonymAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.synonymAnalyzer.filter", "lowercase", "asciifolding", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("synonymAnalyzer"),
            "høj",
            new String[] { "hoj", "height" },
            new int[] { 1, 0 }
        );
    }

    public void testPreconfigured() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "würst, sausage")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "lowercase", "asciifolding", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("my_analyzer"),
            "würst",
            new String[] { "wurst", "sausage" },
            new int[] { 1, 0 }
        );
    }

    public void testChainedSynonymFilters() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms1.type", "synonym")
            .putList("index.analysis.filter.synonyms1.synonyms", "term1, term2")
            .put("index.analysis.filter.synonyms2.type", "synonym")
            .putList("index.analysis.filter.synonyms2.synonyms", "term1, term3")
            .put("index.analysis.analyzer.syn.tokenizer", "standard")
            .putList("index.analysis.analyzer.syn.filter", "lowercase", "synonyms1", "synonyms2")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;

        BaseTokenStreamTestCase.assertAnalyzesTo(
            indexAnalyzers.get("syn"),
            "term1",
            new String[] { "term1", "term3", "term2" },
            new int[] { 1, 0, 0 }
        );
    }

    public void testShingleFilters() {

        Settings settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersion.current())
            )
            .put("path.home", createTempDir().toString())
            .put("index.analysis.filter.synonyms.type", "synonym")
            .putList("index.analysis.filter.synonyms.synonyms", "programmer, developer")
            .put("index.analysis.filter.my_shingle.type", "shingle")
            .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.my_analyzer.filter", "my_shingle", "synonyms")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        expectThrows(IllegalArgumentException.class, () -> {
            indexAnalyzers = createTestAnalysis(idxSettings, settings, new CommonAnalysisPlugin()).indexAnalyzers;
        });

    }

    public void testTokenFiltersBypassSynonymAnalysis() throws IOException {

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("path.home", createTempDir().toString())
            .putList("word_list", "a")
            .put("hyphenation_patterns_path", "foo")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);

        String[] bypassingFactories = new String[] { "dictionary_decompounder" };

        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();
        for (String factory : bypassingFactories) {
            TokenFilterFactory tff = plugin.getTokenFilters().get(factory).get(idxSettings, null, factory, settings);
            TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, null, "keyword", settings);
            Analyzer analyzer = SynonymTokenFilterFactory.buildSynonymAnalyzer(
                tok,
                Collections.emptyList(),
                Collections.singletonList(tff)
            );

            try (TokenStream ts = analyzer.tokenStream("field", "text")) {
                assertThat(ts, instanceOf(KeywordTokenizer.class));
            }
        }

    }

    public void testPreconfiguredTokenFilters() throws IOException {
        Set<String> disallowedFilters = new HashSet<>(
            Arrays.asList("common_grams", "edge_ngram", "keyword_repeat", "ngram", "shingle", "word_delimiter", "word_delimiter_graph")
        );

        Settings settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersion.current())
            )
            .put("path.home", createTempDir().toString())
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        Set<String> disallowedFiltersTested = new HashSet<String>();

        try (CommonAnalysisPlugin plugin = new CommonAnalysisPlugin()) {
            for (PreConfiguredTokenFilter tf : plugin.getPreConfiguredTokenFilters()) {
                if (disallowedFilters.contains(tf.getName())) {
                    IllegalArgumentException e = expectThrows(
                        IllegalArgumentException.class,
                        "Expected exception for factory " + tf.getName(),
                        () -> {
                            tf.get(idxSettings, null, tf.getName(), settings).getSynonymFilter();
                        }
                    );
                    assertEquals(tf.getName(), "Token filter [" + tf.getName() + "] cannot be used to parse synonyms", e.getMessage());
                    disallowedFiltersTested.add(tf.getName());
                } else {
                    tf.get(idxSettings, null, tf.getName(), settings).getSynonymFilter();
                }
            }
        }
        assertEquals("Set of dissallowed filters contains more filters than tested", disallowedFiltersTested, disallowedFilters);
    }

    public void testDisallowedTokenFilters() throws IOException {

        Settings settings = Settings.builder()
            .put(
                IndexMetadata.SETTING_VERSION_CREATED,
                IndexVersionUtils.randomVersionBetween(random(), IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersion.current())
            )
            .put("path.home", createTempDir().toString())
            .putList("common_words", "a", "b")
            .put("output_unigrams", "true")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", settings);
        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();

        String[] disallowedFactories = new String[] {
            "multiplexer",
            "cjk_bigram",
            "common_grams",
            "ngram",
            "edge_ngram",
            "word_delimiter",
            "word_delimiter_graph",
            "fingerprint" };

        for (String factory : disallowedFactories) {
            TokenFilterFactory tff = plugin.getTokenFilters().get(factory).get(idxSettings, null, factory, settings);
            TokenizerFactory tok = new KeywordTokenizerFactory(idxSettings, null, "keyword", settings);

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                "Expected IllegalArgumentException for factory " + factory,
                () -> SynonymTokenFilterFactory.buildSynonymAnalyzer(tok, Collections.emptyList(), Collections.singletonList(tff))
            );

            assertEquals(factory, "Token filter [" + factory + "] cannot be used to parse synonyms", e.getMessage());
        }
    }

    private void match(String analyzerName, String source, String target) throws IOException {
        Analyzer analyzer = indexAnalyzers.get(analyzerName).analyzer();

        TokenStream stream = analyzer.tokenStream("", source);
        stream.reset();
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);

        StringBuilder sb = new StringBuilder();
        while (stream.incrementToken()) {
            sb.append(termAtt.toString()).append(" ");
        }

        MatcherAssert.assertThat(sb.toString().trim(), equalTo(target));
    }

}
