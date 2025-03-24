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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.MyFilterTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.indices.analysis.AnalysisModule.AnalysisProvider;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;

public class CompoundAnalysisTests extends ESTestCase {

    public void testDefaultsCompoundAnalysis() throws Exception {
        Settings settings = getJsonSettings();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        AnalysisModule analysisModule = createAnalysisModule(settings);
        TokenFilterFactory filterFactory = analysisModule.getAnalysisRegistry().buildTokenFilterFactories(idxSettings).get("dict_dec");
        MatcherAssert.assertThat(filterFactory, instanceOf(DictionaryCompoundWordTokenFilterFactory.class));
    }

    public void testDictionaryDecompounder() throws Exception {
        Settings[] settingsArr = new Settings[] { getJsonSettings(), getYamlSettings() };
        for (Settings settings : settingsArr) {
            List<String> terms = analyze(settings, "decompoundingAnalyzer", "donaudampfschiff spargelcremesuppe");
            MatcherAssert.assertThat(terms.size(), equalTo(8));
            MatcherAssert.assertThat(
                terms,
                hasItems("donau", "dampf", "schiff", "donaudampfschiff", "spargel", "creme", "suppe", "spargelcremesuppe")
            );
        }
    }

    public void testHyphenationDecompoundingAnalyzerOnlyLongestMatch() throws Exception {
        Settings[] settingsArr = new Settings[] { getJsonSettings(), getYamlSettings() };
        for (Settings settings : settingsArr) {
            List<String> terms = analyze(settings, "hyphenationDecompoundingAnalyzerOnlyLongestMatch", "kaffeemaschine fussballpumpe");
            MatcherAssert.assertThat(
                terms,
                hasItems("kaffeemaschine", "kaffee", "fee", "maschine", "fussballpumpe", "fussball", "ballpumpe", "pumpe")
            );
        }
    }

    /**
     * For example given a word list of: ["kaffee", "fee", "maschine"]
     * no_sub_matches should prevent the token "fee" as a token in "kaffeemaschine".
     */
    public void testHyphenationDecompoundingAnalyzerNoSubMatches() throws Exception {
        Settings[] settingsArr = new Settings[] { getJsonSettings(), getYamlSettings() };
        for (Settings settings : settingsArr) {
            List<String> terms = analyze(settings, "hyphenationDecompoundingAnalyzerNoSubMatches", "kaffeemaschine fussballpumpe");
            MatcherAssert.assertThat(terms, hasItems("kaffeemaschine", "kaffee", "maschine", "fussballpumpe", "fussball", "ballpumpe"));
        }
    }

    /**
     * For example given a word list of: ["fuss", "fussball", "ballpumpe", "ball", "pumpe"]
     * no_overlapping_matches should prevent the token "ballpumpe" as a token in "fussballpumpe.
     */
    public void testHyphenationDecompoundingAnalyzerNoOverlappingMatches() throws Exception {
        Settings[] settingsArr = new Settings[] { getJsonSettings(), getYamlSettings() };
        for (Settings settings : settingsArr) {
            List<String> terms = analyze(settings, "hyphenationDecompoundingAnalyzerNoOverlappingMatches", "kaffeemaschine fussballpumpe");
            MatcherAssert.assertThat(terms, hasItems("kaffeemaschine", "kaffee", "maschine", "fussballpumpe", "fussball", "pumpe"));
        }
    }

    private List<String> analyze(Settings settings, String analyzerName, String text) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        AnalysisModule analysisModule = createAnalysisModule(settings);
        IndexAnalyzers indexAnalyzers = analysisModule.getAnalysisRegistry().build(IndexCreationContext.CREATE_INDEX, idxSettings);
        Analyzer analyzer = indexAnalyzers.get(analyzerName).analyzer();

        TokenStream stream = analyzer.tokenStream("", text);
        stream.reset();
        CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);

        List<String> terms = new ArrayList<>();
        while (stream.incrementToken()) {
            String tokText = termAtt.toString();
            terms.add(tokText);
        }
        return terms;
    }

    private AnalysisModule createAnalysisModule(Settings settings) throws IOException {
        CommonAnalysisPlugin commonAnalysisPlugin = new CommonAnalysisPlugin();
        return new AnalysisModule(TestEnvironment.newEnvironment(settings), Arrays.asList(commonAnalysisPlugin, new AnalysisPlugin() {
            @Override
            public Map<String, AnalysisProvider<TokenFilterFactory>> getTokenFilters() {
                return singletonMap("myfilter", MyFilterTokenFilterFactory::new);
            }
        }), new StablePluginsRegistry());
    }

    private Settings getJsonSettings() throws IOException {
        return getSettings("/org/elasticsearch/analysis/common/test1.json");
    }

    private Settings getYamlSettings() throws IOException {
        return getSettings("/org/elasticsearch/analysis/common/test1.yml");
    }

    private Settings getSettings(String filePath) throws IOException {
        String hypenationRulesFileName = "de_DR.xml";
        InputStream hypenationRules = getClass().getResourceAsStream(hypenationRulesFileName);
        Path home = createTempDir();
        Path config = home.resolve("config");
        Files.createDirectory(config);
        Files.copy(hypenationRules, config.resolve(hypenationRulesFileName));

        return Settings.builder()
            .loadFromStream(filePath, getClass().getResourceAsStream(filePath), false)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(Environment.PATH_HOME_SETTING.getKey(), home.toString())
            .build();
    }
}
