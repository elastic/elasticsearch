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

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.test.ESTokenStreamTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;

public class MultiplexerTokenFilterTests extends ESTokenStreamTestCase {

    public void testMultiplexingFilter() throws IOException {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put("index.analysis.filter.t.type", "truncate")
            .put("index.analysis.filter.t.length", "2")
            .put("index.analysis.filter.multiplexFilter.type", "multiplexer")
            .putList("index.analysis.filter.multiplexFilter.filters", "identity", "lowercase, t", "uppercase")
            .put("index.analysis.analyzer.myAnalyzer.type", "custom")
            .put("index.analysis.analyzer.myAnalyzer.tokenizer", "standard")
            .putList("index.analysis.analyzer.myAnalyzer.filter", "multiplexFilter")
            .build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("index", indexSettings);

        IndexAnalyzers indexAnalyzers = new AnalysisModule(TestEnvironment.newEnvironment(settings),
            Collections.singletonList(new CommonAnalysisPlugin())).getAnalysisRegistry().build(idxSettings);

        try (NamedAnalyzer analyzer = indexAnalyzers.get("myAnalyzer")) {
            assertNotNull(analyzer);
            TokenStream tokenStream = analyzer.tokenStream("foo", "ONe tHree");
            tokenStream.reset();
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posIncAtt = tokenStream.addAttribute(PositionIncrementAttribute.class);
            assertTrue(tokenStream.incrementToken());
            assertEquals("ONe", charTermAttribute.toString());
            assertEquals(1, posIncAtt.getPositionIncrement());
            assertTrue(tokenStream.incrementToken());
            assertEquals("on", charTermAttribute.toString());
            assertEquals(0, posIncAtt.getPositionIncrement());
            assertTrue(tokenStream.incrementToken());
            assertEquals("ONE", charTermAttribute.toString());
            assertEquals(0, posIncAtt.getPositionIncrement());
            assertTrue(tokenStream.incrementToken());
            assertEquals("tHree", charTermAttribute.toString());
            assertEquals(1, posIncAtt.getPositionIncrement());
            assertTrue(tokenStream.incrementToken());
            assertEquals("th", charTermAttribute.toString());
            assertEquals(0, posIncAtt.getPositionIncrement());
            assertTrue(tokenStream.incrementToken());
            assertEquals("THREE", charTermAttribute.toString());
            assertEquals(0, posIncAtt.getPositionIncrement());
            assertFalse(tokenStream.incrementToken());
        }
    }

}
