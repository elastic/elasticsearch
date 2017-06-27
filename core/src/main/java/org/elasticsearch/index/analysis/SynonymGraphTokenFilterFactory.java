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
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.io.Reader;

public class SynonymGraphTokenFilterFactory extends SynonymTokenFilterFactory {
    public SynonymGraphTokenFilterFactory(IndexSettings indexSettings, Environment env, AnalysisRegistry analysisRegistry,
                                     String name, Settings settings) throws IOException {
        super(indexSettings, env, analysisRegistry, name, settings);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        throw new IllegalStateException("Call createPerAnalyzerSynonymGraphFactory to specialize this factory for an analysis chain first");
    }

    Factory createPerAnalyzerSynonymGraphFactory(Analyzer analyzerForParseSynonym, Environment env){
        return new Factory("synonymgraph", analyzerForParseSynonym, getRulesFromSettings(env));
    }

    public class Factory implements TokenFilterFactory{

        private final String name;
        private final SynonymMap synonymMap;

        public Factory(String name, final Analyzer analyzerForParseSynonym, Reader rulesReader) {
            this.name = name;

            try {
                SynonymMap.Builder parser;
                if ("wordnet".equalsIgnoreCase(format)) {
                    parser = new WordnetSynonymParser(true, expand, analyzerForParseSynonym);
                    ((WordnetSynonymParser) parser).parse(rulesReader);
                } else {
                    parser = new SolrSynonymParser(true, expand, analyzerForParseSynonym);
                    ((SolrSynonymParser) parser).parse(rulesReader);
                }
                synonymMap = parser.build();
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to build synonyms", e);
            }
        }

        @Override
        public String name() {
            return this.name;
        }

        @Override
        public TokenStream create(TokenStream tokenStream) {
            // fst is null means no synonyms
            return synonymMap.fst == null ? tokenStream : new SynonymGraphFilter(tokenStream, synonymMap, ignoreCase);
        }
    }
}
