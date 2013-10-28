/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class PreBuiltAnalyzerTests extends ElasticsearchTestCase {

    @Test
    public void testThatDefaultAndStandardAnalyzerAreTheSameInstance() {
        Analyzer currentStandardAnalyzer = PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT);
        Analyzer currentDefaultAnalyzer = PreBuiltAnalyzers.DEFAULT.getAnalyzer(Version.CURRENT);

        // special case, these two are the same instance
        assertThat(currentDefaultAnalyzer, is(currentStandardAnalyzer));
    }

    @Test
    public void testThatInstancesAreTheSameAlwaysForKeywordAnalyzer() {
        assertThat(PreBuiltAnalyzers.KEYWORD.getAnalyzer(Version.CURRENT),
                is(PreBuiltAnalyzers.KEYWORD.getAnalyzer(Version.V_0_18_0)));
    }

    @Test
    public void testThatInstancesAreCachedAndReused() {
        assertThat(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.CURRENT),
                is(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.CURRENT)));
        assertThat(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.V_0_18_0),
                is(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.V_0_18_0)));
    }

    @Test
    public void testThatInstancesWithSameLuceneVersionAreReused() {
        // both are lucene 4.4 and should return the same instance
        assertThat(PreBuiltAnalyzers.CATALAN.getAnalyzer(Version.V_0_90_4),
                is(PreBuiltAnalyzers.CATALAN.getAnalyzer(Version.V_0_90_5)));
    }

    @Test
    public void testThatAnalyzersAreUsedInMapping() throws IOException {
        int randomInt = randomInt(PreBuiltAnalyzers.values().length-1);
        PreBuiltAnalyzers randomPreBuiltAnalyzer = PreBuiltAnalyzers.values()[randomInt];
        String analyzerName = randomPreBuiltAnalyzer.name().toLowerCase(Locale.ROOT);

        Version randomVersion = randomVersion();
        Settings indexSettings = ImmutableSettings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, randomVersion).build();

        NamedAnalyzer namedAnalyzer = new PreBuiltAnalyzerProvider(analyzerName, AnalyzerScope.INDEX, randomPreBuiltAnalyzer.getAnalyzer(randomVersion)).get();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("analyzer", analyzerName).endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = MapperTestUtils.newParser(indexSettings).parse(mapping);

        FieldMapper fieldMapper = docMapper.mappers().name("field").mapper();
        assertThat(fieldMapper.searchAnalyzer(), instanceOf(NamedAnalyzer.class));
        NamedAnalyzer fieldMapperNamedAnalyzer = (NamedAnalyzer) fieldMapper.searchAnalyzer();

        assertThat(fieldMapperNamedAnalyzer.analyzer(), is(namedAnalyzer.analyzer()));
    }
}
