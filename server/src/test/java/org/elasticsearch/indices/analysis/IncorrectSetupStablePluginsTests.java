/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.analysis;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.indices.analysis.lucene.ReplaceCharToNumber;
import org.elasticsearch.plugin.Inject;
import org.elasticsearch.plugin.NamedComponent;
import org.elasticsearch.plugin.analysis.CharFilterFactory;
import org.elasticsearch.plugin.settings.AnalysisSettings;
import org.elasticsearch.plugins.scanners.NameToPluginInfo;
import org.elasticsearch.plugins.scanners.NamedComponentReader;
import org.elasticsearch.plugins.scanners.PluginInfo;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.io.Reader;
import java.util.Map;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;

public class IncorrectSetupStablePluginsTests extends ESTestCase {
    ClassLoader classLoader = getClass().getClassLoader();

    private final Settings emptyNodeSettings = Settings.builder()
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    public @interface IncorrectAnnotation {
    }

    @IncorrectAnnotation
    public interface IncorrectlyAnnotatedSettings {}

    @NamedComponent("incorrectlyAnnotatedSettings")
    public static class IncorrectlyAnnotatedSettingsCharFilter extends AbstractCharFilterFactory {
        @Inject
        public IncorrectlyAnnotatedSettingsCharFilter(IncorrectlyAnnotatedSettings settings) {}
    }

    public void testIncorrectlyAnnotatedSettingsClass() throws IOException {
        var e = expectThrows(
            IllegalArgumentException.class,
            () -> getIndexAnalyzers(
                Settings.builder()
                    .put("index.analysis.analyzer.char_filter_test.tokenizer", "standard")
                    .put("index.analysis.analyzer.char_filter_test.char_filter", "incorrectlyAnnotatedSettings")
                    .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                    .build(),
                Map.of(
                    "incorrectlyAnnotatedSettings",
                    new PluginInfo("incorrectlyAnnotatedSettings", IncorrectlyAnnotatedSettingsCharFilter.class.getName(), classLoader)
                )
            )
        );
        assertThat(e.getMessage(), equalTo("Parameter is not instance of a class annotated with settings annotation."));
    }

    @AnalysisSettings
    public interface OkAnalysisSettings {}

    @NamedComponent("noInjectCharFilter")
    public static class NoInjectCharFilter extends AbstractCharFilterFactory {

        public NoInjectCharFilter(OkAnalysisSettings settings) {}
    }

    public void testIncorrectlyAnnotatedConstructor() throws IOException {
        var e = expectThrows(
            IllegalStateException.class,
            () -> getIndexAnalyzers(
                Settings.builder()
                    .put("index.analysis.analyzer.char_filter_test.tokenizer", "standard")
                    .put("index.analysis.analyzer.char_filter_test.char_filter", "noInjectCharFilter")
                    .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                    .build(),
                Map.of("noInjectCharFilter", new PluginInfo("noInjectCharFilter", NoInjectCharFilter.class.getName(), classLoader))
            )
        );
        assertThat(e.getMessage(), equalTo("Missing @Inject annotation for constructor with settings."));
    }

    @NamedComponent("multipleConstructors")
    public static class MultipleConstructors extends AbstractCharFilterFactory {
        public MultipleConstructors() {}

        public MultipleConstructors(OkAnalysisSettings settings) {}
    }

    public void testMultiplePublicConstructors() throws IOException {
        var e = expectThrows(
            IllegalStateException.class,
            () -> getIndexAnalyzers(
                Settings.builder()
                    .put("index.analysis.analyzer.char_filter_test.tokenizer", "standard")
                    .put("index.analysis.analyzer.char_filter_test.char_filter", "multipleConstructors")
                    .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
                    .build(),
                Map.of("multipleConstructors", new PluginInfo("multipleConstructors", MultipleConstructors.class.getName(), classLoader))
            )
        );
        assertThat(e.getMessage(), equalTo("Plugin can only have one public constructor."));
    }

    public IndexAnalyzers getIndexAnalyzers(Settings settings, Map<String, PluginInfo> mapOfCharFilters) throws IOException {
        AnalysisRegistry registry = setupRegistry(mapOfCharFilters);

        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        return registry.build(IndexCreationContext.CREATE_INDEX, idxSettings);
    }

    private AnalysisRegistry setupRegistry(Map<String, PluginInfo> mapOfCharFilters) throws IOException {

        AnalysisRegistry registry = new AnalysisModule(
            TestEnvironment.newEnvironment(emptyNodeSettings),
            emptyList(),
            new StablePluginsRegistry(
                new NamedComponentReader(),
                Map.of(CharFilterFactory.class.getCanonicalName(), new NameToPluginInfo(mapOfCharFilters))
            )
        ).getAnalysisRegistry();
        return registry;
    }

    public abstract static class AbstractCharFilterFactory implements CharFilterFactory {

        @Override
        public Reader create(Reader reader) {
            return new ReplaceCharToNumber(reader, "#", 3);
        }

        @Override
        public Reader normalize(Reader reader) {
            return new ReplaceCharToNumber(reader, "#", 3);
        }
    }
}
