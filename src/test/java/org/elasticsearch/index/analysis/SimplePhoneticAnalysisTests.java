package org.elasticsearch.index.analysis;

import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.hamcrest.MatcherAssert;
import static org.hamcrest.Matchers.*;
import org.testng.annotations.Test;

/**
 */
public class SimplePhoneticAnalysisTests {

    @Test
    public void testPhoneticTokenFilterFactory() {
        Settings settings = settingsBuilder().loadFromClasspath("org/elasticsearch/index/analysis/phonetic-1.yml").build();
        AnalysisService analysisService = testSimpleConfiguration(settings);
        TokenFilterFactory standardfilterFactory = analysisService.tokenFilter("standard");
        System.err.println("standard filterfactory = " + standardfilterFactory);
        TokenFilterFactory filterFactory = analysisService.tokenFilter("phonetic");
        System.err.println("filterfactory = " + filterFactory);
        MatcherAssert.assertThat(filterFactory, instanceOf(PhoneticTokenFilterFactory.class));
    }

    private AnalysisService testSimpleConfiguration(Settings settings) {
        Index index = new Index("test");

        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings),
                new EnvironmentModule(new Environment(settings)),
                new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, settings),
                new IndexNameModule(index),
                new AnalysisModule(settings, parentInjector.getInstance(IndicesAnalysisService.class))
                .addProcessor(new PhoneticAnalysisBinderProcessor())).createChildInjector(parentInjector);

        AnalysisService analysisService = injector.getInstance(AnalysisService.class);
        return analysisService;
    }
}
