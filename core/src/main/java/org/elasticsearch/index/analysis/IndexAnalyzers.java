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

import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;

import java.io.Closeable;
import java.util.Map;

/**
 * IndexAnalyzers contains a name to analyzer mapping for a specific index.
 * This class only holds analyzers that are explicitly configured for an index and doesn't allow
 * access to individual tokenizers, char or token filter.
 *
 * @see AnalysisRegistry
 */
public final class IndexAnalyzers extends AbstractIndexComponent implements Closeable {
    private final NamedAnalyzer defaultIndexAnalyzer;
    private final NamedAnalyzer defaultSearchAnalyzer;
    private final NamedAnalyzer defaultSearchQuoteAnalyzer;
    private final Map<String, NamedAnalyzer> analyzers;
    private final IndexSettings indexSettings;

    public IndexAnalyzers(IndexSettings indexSettings, NamedAnalyzer defaultIndexAnalyzer, NamedAnalyzer defaultSearchAnalyzer,
                          NamedAnalyzer defaultSearchQuoteAnalyzer, Map<String, NamedAnalyzer> analyzers) {
        super(indexSettings);
        this.defaultIndexAnalyzer = defaultIndexAnalyzer;
        this.defaultSearchAnalyzer = defaultSearchAnalyzer;
        this.defaultSearchQuoteAnalyzer = defaultSearchQuoteAnalyzer;
        this.analyzers = analyzers;
        this.indexSettings = indexSettings;
    }

    /**
     * Returns an analyzer mapped to the given name or <code>null</code> if not present
     */
    public NamedAnalyzer get(String name) {
        return analyzers.get(name);
    }


    /**
     * Returns the default index analyzer for this index
     */
    public NamedAnalyzer getDefaultIndexAnalyzer() {
        return defaultIndexAnalyzer;
    }

    /**
     * Returns the default search analyzer for this index
     */
    public NamedAnalyzer getDefaultSearchAnalyzer() {
        return defaultSearchAnalyzer;
    }

    /**
     * Returns the default search quote analyzer for this index
     */
    public NamedAnalyzer getDefaultSearchQuoteAnalyzer() {
        return defaultSearchQuoteAnalyzer;
    }

    @Override
    public void close() {
        for (NamedAnalyzer analyzer : analyzers.values()) {
            if (analyzer.scope() == AnalyzerScope.INDEX) {
                try {
                    analyzer.close();
                } catch (NullPointerException e) {
                    // because analyzers are aliased, they might be closed several times
                    // an NPE is thrown in this case, so ignore....
                    // TODO: Analyzer's can no longer have aliases in indices created in 5.x and beyond,
                    // so we only allow the aliases for analyzers on indices created pre 5.x for backwards
                    // compatibility.  Once pre 5.0 indices are no longer supported, this check should be removed.
                } catch (Exception e) {
                    logger.debug("failed to close analyzer {}", analyzer);
                }
            }
        }
    }

    /**
     * Returns the indices settings
     */
    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

}
