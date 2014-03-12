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

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTokenStreamTestCase;
import org.junit.Ignore;

import java.io.IOException;

import static com.carrotsearch.randomizedtesting.RandomizedTest.scaledRandomIntBetween;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;

/**
 */
public class AnalyzerBackwardsCompatTests extends ElasticsearchTokenStreamTestCase {

    @Ignore
    private void testNoStopwordsAfter(org.elasticsearch.Version noStopwordVersion, String type) throws IOException {
        final int iters = scaledRandomIntBetween(10, 100);
        org.elasticsearch.Version version = org.elasticsearch.Version.CURRENT;
        for (int i = 0; i < iters; i++) {
            ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put("index.analysis.filter.my_stop.type", "stop");
            if (version.onOrAfter(noStopwordVersion))  {
                if (random().nextBoolean()) {
                    builder.put(SETTING_VERSION_CREATED, version);
                }
            } else {
                builder.put(SETTING_VERSION_CREATED, version);
            }
            builder.put("index.analysis.analyzer.foo.type", type);
            AnalysisService analysisService = AnalysisTestsHelper.createAnalysisServiceFromSettings(builder.build());
            NamedAnalyzer analyzer = analysisService.analyzer("foo");
            if (version.onOrAfter(noStopwordVersion)) {
                assertAnalyzesTo(analyzer, "this is bogus", new String[]{"this", "is", "bogus"});
            } else {
                assertAnalyzesTo(analyzer, "this is bogus", new String[]{"bogus"});
            }
            version = randomVersion();
        }
    }

    public void testPatternAnalyzer() throws IOException {
        testNoStopwordsAfter(org.elasticsearch.Version.V_1_0_0_RC1, "pattern");
    }

    public void testStandardHTMLStripAnalyzer() throws IOException {
        testNoStopwordsAfter(org.elasticsearch.Version.V_1_0_0_RC1, "standard_html_strip");
    }

    public void testStandardAnalyzer() throws IOException {
        testNoStopwordsAfter(org.elasticsearch.Version.V_1_0_0_Beta1, "standard");
    }
}
