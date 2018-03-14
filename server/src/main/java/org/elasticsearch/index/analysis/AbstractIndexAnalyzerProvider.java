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
import org.apache.lucene.util.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;

public abstract class AbstractIndexAnalyzerProvider<T extends Analyzer> extends AbstractIndexComponent implements AnalyzerProvider<T> {
    protected final Version version;

    /**
     * Constructs a new analyzer component, with the index name and its settings and the analyzer name.
     *
     * @param indexSettings the settings and the name of the index
     */
    // TODO drop ignored
    public AbstractIndexAnalyzerProvider(IndexSettings indexSettings, String ignored, Settings settings) {
        super(indexSettings);
        this.version = Analysis.parseAnalysisVersion(this.indexSettings.getSettings(), settings, logger);
    }


    @Override
    public final AnalyzerScope scope() {
        return AnalyzerScope.INDEX;
    }
}
