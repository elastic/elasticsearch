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
import org.elasticsearch.indices.analysis.PreBuiltAnalyzers;

/**
 *
 */
public class PreBuiltAnalyzerProviderFactory implements AnalyzerProviderFactory {

    private final PreBuiltAnalyzerProvider analyzerProvider;

    public PreBuiltAnalyzerProviderFactory(String name, AnalyzerScope scope, Analyzer analyzer) {
        analyzerProvider = new PreBuiltAnalyzerProvider(name, scope, analyzer);
    }

    @Override
    public AnalyzerProvider create(String name, Settings settings) {
        Version indexVersion = Version.indexCreated(settings);
        if (!Version.CURRENT.equals(indexVersion)) {
            PreBuiltAnalyzers preBuiltAnalyzers = PreBuiltAnalyzers.getOrDefault(name, null);
            if (preBuiltAnalyzers != null) {
                Analyzer analyzer = preBuiltAnalyzers.getAnalyzer(indexVersion);
                return new PreBuiltAnalyzerProvider(name, AnalyzerScope.INDICES, analyzer);
            }
        }

        return analyzerProvider;
    }

    public Analyzer analyzer() {
        return analyzerProvider.get();
    }
}
