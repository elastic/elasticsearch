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

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class CjkAnalyzerProvider extends AbstractIndexAnalyzerProvider<CJKAnalyzer> {

    private final CJKAnalyzer analyzer;

    public CjkAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        CharArraySet stopWords = Analysis.parseStopWords(
            env, indexSettings.getIndexVersionCreated(), settings, CJKAnalyzer.getDefaultStopSet());

        analyzer = new CJKAnalyzer(stopWords);
        analyzer.setVersion(version);
    }

    @Override
    public CJKAnalyzer get() {
        return this.analyzer;
    }
}
