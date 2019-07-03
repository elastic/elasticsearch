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

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexAnalyzersTests extends ESTestCase {

    public void testClose() throws IOException {

        AtomicInteger closes = new AtomicInteger(0);
        NamedAnalyzer a = new NamedAnalyzer("default", AnalyzerScope.INDEX, new WhitespaceAnalyzer()){
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        NamedAnalyzer n = new NamedAnalyzer("keyword_normalizer", AnalyzerScope.INDEX, new KeywordAnalyzer()){
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        NamedAnalyzer w = new NamedAnalyzer("whitespace_normalizer", AnalyzerScope.INDEX, new WhitespaceAnalyzer()){
            @Override
            public void close() {
                super.close();
                closes.incrementAndGet();
            }
        };

        Index index = new Index("_index", "testUUID");
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, Settings.EMPTY);
        NamedAnalyzer namedAnalyzer = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());

        IndexAnalyzers ia = new IndexAnalyzers(indexSettings, namedAnalyzer, namedAnalyzer, namedAnalyzer,
            Collections.singletonMap("default", a), Collections.singletonMap("n", n), Collections.singletonMap("w", w));
        ia.close();
        assertEquals(3, closes.get());

    }

}
