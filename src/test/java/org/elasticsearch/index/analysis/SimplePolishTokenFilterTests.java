package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.pl.PolishAnalysisBinderProcessor;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.Matchers.equalTo;

public class SimplePolishTokenFilterTests extends ElasticsearchTestCase {

    @Test
    public void testBasicUsage() throws Exception {
        testToken("kwiaty", "kwć");
        testToken("canona", "ć");
        testToken("wirtualna", "wirtualny");
        testToken("polska", "polski");

        testAnalyzer("wirtualna polska", "wirtualny", "polski");
    }

    private void testToken(String source, String expected) throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.analysis.filter.myStemmer.type", "polish_stem")
                .build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        TokenFilterFactory filterFactory = analysisService.tokenFilter("myStemmer");

        TokenStream ts = filterFactory.create(new KeywordTokenizer(new StringReader(source)));

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        assertThat(ts.incrementToken(), equalTo(true));

        assertThat(term1.toString(), equalTo(expected));
    }

    private void testAnalyzer(String source, String... expected_terms) throws IOException {
        Index index = new Index("test");
        Settings settings = ImmutableSettings.settingsBuilder().build();
        AnalysisService analysisService = createAnalysisService(index, settings);

        Analyzer analyzer = analysisService.analyzer("polish").analyzer();

        TokenStream ts = analyzer.tokenStream("test", source);

        CharTermAttribute term1 = ts.addAttribute(CharTermAttribute.class);
        ts.reset();

        for (String expected : expected_terms) {
            assertThat(ts.incrementToken(), equalTo(true));
            assertThat(term1.toString(), equalTo(expected));
        }
    }

    private AnalysisService createAnalysisService(Index index, Settings settings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings), new EnvironmentModule(new Environment(settings)), new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class)).addProcessor(new PolishAnalysisBinderProcessor()))
                .createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }
}
