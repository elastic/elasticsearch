/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
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

package org.elasticsearch.indices.analysis.pl;

import org.apache.lucene.analysis.Analyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProvider;
import org.elasticsearch.index.analysis.PreBuiltAnalyzerProviderFactory;

public class StempelAnalyzerProviderFactory extends PreBuiltAnalyzerProviderFactory {

    private final PreBuiltAnalyzerProvider analyzerProvider;

    public StempelAnalyzerProviderFactory(String name, AnalyzerScope scope, Analyzer analyzer) {
        super(name, scope, analyzer);
        analyzerProvider = new PreBuiltAnalyzerProvider(name, scope, analyzer);
    }

    @Override
    public AnalyzerProvider create(String name, Settings settings) {
        return analyzerProvider;
    }

    public Analyzer analyzer() {
        return analyzerProvider.get();
    }
}
