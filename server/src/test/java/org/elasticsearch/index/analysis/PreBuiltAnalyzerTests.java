/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PreBuiltAnalyzerTests extends ESSingleNodeTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class);
    }

    public void testThatDefaultAndStandardAnalyzerAreTheSameInstance() {
        Analyzer currentStandardAnalyzer = PreBuiltAnalyzers.STANDARD.getAnalyzer(IndexVersion.current());
        Analyzer currentDefaultAnalyzer = PreBuiltAnalyzers.DEFAULT.getAnalyzer(IndexVersion.current());

        // special case, these two are the same instance
        assertThat(currentDefaultAnalyzer, is(currentStandardAnalyzer));
    }

    public void testThatInstancesAreTheSameAlwaysForKeywordAnalyzer() {
        assertThat(
            PreBuiltAnalyzers.KEYWORD.getAnalyzer(IndexVersion.current()),
            is(PreBuiltAnalyzers.KEYWORD.getAnalyzer(IndexVersion.MINIMUM_COMPATIBLE))
        );
    }

    public void testThatInstancesAreCachedAndReused() {
        assertSame(
            PreBuiltAnalyzers.STANDARD.getAnalyzer(IndexVersion.current()),
            PreBuiltAnalyzers.STANDARD.getAnalyzer(IndexVersion.current())
        );
        // same index version should be cached
        IndexVersion v = IndexVersionUtils.randomVersion(random());
        assertSame(PreBuiltAnalyzers.STANDARD.getAnalyzer(v), PreBuiltAnalyzers.STANDARD.getAnalyzer(v));
        assertNotSame(
            PreBuiltAnalyzers.STANDARD.getAnalyzer(IndexVersion.current()),
            PreBuiltAnalyzers.STANDARD.getAnalyzer(IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersion.current()))
        );

        // Same Lucene version should be cached:
        assertSame(PreBuiltAnalyzers.STOP.getAnalyzer(IndexVersion.V_8_0_0), PreBuiltAnalyzers.STOP.getAnalyzer(IndexVersion.V_8_0_1));
    }

    public void testThatAnalyzersAreUsedInMapping() throws IOException {
        int randomInt = randomInt(PreBuiltAnalyzers.values().length - 1);
        PreBuiltAnalyzers randomPreBuiltAnalyzer = PreBuiltAnalyzers.values()[randomInt];
        String analyzerName = randomPreBuiltAnalyzer.name().toLowerCase(Locale.ROOT);

        IndexVersion randomVersion = IndexVersionUtils.randomVersion(random());
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, randomVersion.id()).build();

        NamedAnalyzer namedAnalyzer = new PreBuiltAnalyzerProvider(
            analyzerName,
            AnalyzerScope.INDEX,
            randomPreBuiltAnalyzer.getAnalyzer(randomVersion)
        ).get();

        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("analyzer", analyzerName)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        MapperService mapperService = createIndex("test", indexSettings, mapping).mapperService();

        MappedFieldType fieldType = mapperService.fieldType("field");
        assertThat(fieldType.getTextSearchInfo().searchAnalyzer(), instanceOf(NamedAnalyzer.class));
        NamedAnalyzer fieldMapperNamedAnalyzer = fieldType.getTextSearchInfo().searchAnalyzer();

        assertThat(fieldMapperNamedAnalyzer.analyzer(), is(namedAnalyzer.analyzer()));
    }
}
