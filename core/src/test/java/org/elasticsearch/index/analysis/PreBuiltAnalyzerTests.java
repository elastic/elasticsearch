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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 *
 */
public class PreBuiltAnalyzerTests extends ESSingleNodeTestCase {
    public void testThatDefaultAndStandardAnalyzerAreTheSameInstance() {
        Analyzer currentStandardAnalyzer = PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.CURRENT);
        Analyzer currentDefaultAnalyzer = PreBuiltAnalyzers.DEFAULT.getAnalyzer(Version.CURRENT);

        // special case, these two are the same instance
        assertThat(currentDefaultAnalyzer, is(currentStandardAnalyzer));
    }

    public void testThatDefaultAndStandardAnalyzerChangedIn10Beta1() throws IOException {
        Analyzer currentStandardAnalyzer = PreBuiltAnalyzers.STANDARD.getAnalyzer(Version.V_1_0_0_Beta1);
        Analyzer currentDefaultAnalyzer = PreBuiltAnalyzers.DEFAULT.getAnalyzer(Version.V_1_0_0_Beta1);

        // special case, these two are the same instance
        assertThat(currentDefaultAnalyzer, is(currentStandardAnalyzer));
        PreBuiltAnalyzers.DEFAULT.getAnalyzer(Version.V_1_0_0_Beta1);
        final int n = scaledRandomIntBetween(10, 100);
        Version version = Version.CURRENT;
        for(int i = 0; i < n; i++) {
            if (version.equals(Version.V_1_0_0_Beta1)) {
                assertThat(currentDefaultAnalyzer, is(PreBuiltAnalyzers.DEFAULT.getAnalyzer(version)));
            } else {
                assertThat(currentDefaultAnalyzer, not(is(PreBuiltAnalyzers.DEFAULT.getAnalyzer(version))));
            }
            Analyzer analyzer = PreBuiltAnalyzers.DEFAULT.getAnalyzer(version);
            TokenStream ts = analyzer.tokenStream("foo", "This is it Dude");
            ts.reset();
            CharTermAttribute charTermAttribute = ts.addAttribute(CharTermAttribute.class);
            List<String> list = new ArrayList<>();
            while(ts.incrementToken()) {
                list.add(charTermAttribute.toString());
            }
            if (version.onOrAfter(Version.V_1_0_0_Beta1)) {
                assertThat(list.size(), is(4));
                assertThat(list, contains("this", "is", "it", "dude"));

            } else {
                assertThat(list.size(), is(1));
                assertThat(list, contains("dude"));
            }
            ts.close();
            version = randomVersion(random());
        }
    }

    public void testAnalyzerChangedIn10RC1() throws IOException {
        Analyzer pattern = PreBuiltAnalyzers.PATTERN.getAnalyzer(Version.V_1_0_0_RC1);
        Analyzer standardHtml = PreBuiltAnalyzers.STANDARD_HTML_STRIP.getAnalyzer(Version.V_1_0_0_RC1);
        final int n = scaledRandomIntBetween(10, 100);
        Version version = Version.CURRENT;
        for(int i = 0; i < n; i++) {
            if (version.equals(Version.V_1_0_0_RC1)) {
                assertThat(pattern, is(PreBuiltAnalyzers.PATTERN.getAnalyzer(version)));
                assertThat(standardHtml, is(PreBuiltAnalyzers.STANDARD_HTML_STRIP.getAnalyzer(version)));
            } else {
                assertThat(pattern, not(is(PreBuiltAnalyzers.DEFAULT.getAnalyzer(version))));
                assertThat(standardHtml, not(is(PreBuiltAnalyzers.DEFAULT.getAnalyzer(version))));
            }
            Analyzer analyzer = randomBoolean() ? PreBuiltAnalyzers.PATTERN.getAnalyzer(version) :  PreBuiltAnalyzers.STANDARD_HTML_STRIP.getAnalyzer(version);
            TokenStream ts = analyzer.tokenStream("foo", "This is it Dude");
            ts.reset();
            CharTermAttribute charTermAttribute = ts.addAttribute(CharTermAttribute.class);
            List<String> list = new ArrayList<>();
            while(ts.incrementToken()) {
                list.add(charTermAttribute.toString());
            }
            if (version.onOrAfter(Version.V_1_0_0_RC1)) {
                assertThat(list.toString(), list.size(), is(4));
                assertThat(list, contains("this", "is", "it", "dude"));

            } else {
                assertThat(list.size(), is(1));
                assertThat(list, contains("dude"));
            }
            ts.close();
            version = randomVersion(random());
        }
    }

    public void testThatInstancesAreTheSameAlwaysForKeywordAnalyzer() {
        assertThat(PreBuiltAnalyzers.KEYWORD.getAnalyzer(Version.CURRENT),
                is(PreBuiltAnalyzers.KEYWORD.getAnalyzer(Version.V_0_18_0)));
    }

    public void testThatInstancesAreCachedAndReused() {
        assertThat(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.CURRENT),
                is(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.CURRENT)));
        assertThat(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.V_0_18_0),
                is(PreBuiltAnalyzers.ARABIC.getAnalyzer(Version.V_0_18_0)));
    }

    public void testThatInstancesWithSameLuceneVersionAreReused() {
        // both are lucene 4.4 and should return the same instance
        assertThat(PreBuiltAnalyzers.CATALAN.getAnalyzer(Version.V_0_90_4),
                is(PreBuiltAnalyzers.CATALAN.getAnalyzer(Version.V_0_90_5)));
    }

    public void testThatAnalyzersAreUsedInMapping() throws IOException {
        int randomInt = randomInt(PreBuiltAnalyzers.values().length-1);
        PreBuiltAnalyzers randomPreBuiltAnalyzer = PreBuiltAnalyzers.values()[randomInt];
        String analyzerName = randomPreBuiltAnalyzer.name().toLowerCase(Locale.ROOT);

        Version randomVersion = randomVersion(random());
        Settings indexSettings = Settings.settingsBuilder().put(IndexMetaData.SETTING_VERSION_CREATED, randomVersion).build();

        NamedAnalyzer namedAnalyzer = new PreBuiltAnalyzerProvider(analyzerName, AnalyzerScope.INDEX, randomPreBuiltAnalyzer.getAnalyzer(randomVersion)).get();

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").startObject("field").field("type", "string").field("analyzer", analyzerName).endObject().endObject()
                .endObject().endObject().string();
        DocumentMapper docMapper = createIndex("test", indexSettings).mapperService().documentMapperParser().parse("type", new CompressedXContent(mapping));

        FieldMapper fieldMapper = docMapper.mappers().getMapper("field");
        assertThat(fieldMapper.fieldType().searchAnalyzer(), instanceOf(NamedAnalyzer.class));
        NamedAnalyzer fieldMapperNamedAnalyzer = fieldMapper.fieldType().searchAnalyzer();

        assertThat(fieldMapperNamedAnalyzer.analyzer(), is(namedAnalyzer.analyzer()));
    }
}
