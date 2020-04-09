/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Locale;

import static org.elasticsearch.test.VersionUtils.randomVersion;
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
        Analyzer currentStandardAnalyzer = PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT);
        Analyzer currentDefaultAnalyzer = PreBuiltAnalyzers.DEFAULT.getAnalyzer(Version.CURRENT);

        // special case, these two are the same instance
        assertThat(currentDefaultAnalyzer, is(currentStandardAnalyzer));
    }

    public void testThatInstancesAreTheSameAlwaysForKeywordAnalyzer() {
        assertThat(PreBuiltAnalyzers.KEYWORD.getAnalyzer(Version.CURRENT),
                is(PreBuiltAnalyzers.KEYWORD.getAnalyzer(Version.CURRENT.minimumIndexCompatibilityVersion())));
    }

    public void testThatInstancesAreCachedAndReused() {
        assertSame(PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT),
                PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT));
        // same es version should be cached
        Version v = VersionUtils.randomVersion(random());
        assertSame(PreBuiltAnalyzers.STANDARD.getAnalyzer(v), PreBuiltAnalyzers.STANDARD.getAnalyzer(v));
        assertNotSame(PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT),
                PreBuiltAnalyzers.STANDARD.getAnalyzer(VersionUtils.randomPreviousCompatibleVersion(random(), Version.CURRENT)));

        // Same Lucene version should be cached:
        assertSame(PreBuiltAnalyzers.STOP.getAnalyzer(Version.fromString("5.0.0")),
            PreBuiltAnalyzers.STOP.getAnalyzer(Version.fromString("5.0.1")));
    }

    public void testThatAnalyzersAreUsedInMapping() throws IOException {
        int randomInt = randomInt(PreBuiltAnalyzers.values().length-1);
        PreBuiltAnalyzers randomPreBuiltAnalyzer = PreBuiltAnalyzers.values()[randomInt];
        String analyzerName = randomPreBuiltAnalyzer.name().toLowerCase(Locale.ROOT);

        Version randomVersion = randomVersion(random());
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, randomVersion).build();

        NamedAnalyzer namedAnalyzer = new PreBuiltAnalyzerProvider(analyzerName, AnalyzerScope.INDEX,
            randomPreBuiltAnalyzer.getAnalyzer(randomVersion)).get();

        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("_doc")
                .startObject("properties").startObject("field").field("type", "text")
                .field("analyzer", analyzerName).endObject().endObject().endObject().endObject();
        MapperService mapperService = createIndex("test", indexSettings, mapping).mapperService();

        MappedFieldType fieldType = mapperService.fieldType("field");
        assertThat(fieldType.searchAnalyzer(), instanceOf(NamedAnalyzer.class));
        NamedAnalyzer fieldMapperNamedAnalyzer = fieldType.searchAnalyzer();

        assertThat(fieldMapperNamedAnalyzer.analyzer(), is(namedAnalyzer.analyzer()));
    }
}
