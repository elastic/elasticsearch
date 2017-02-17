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
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
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
        // fst is null means no synonyms
        return synonymMap.fst == null ? tokenStream : new SynonymGraphFilter(tokenStream, synonymMap, ignoreCase);
    }

    public Factory createPerAnalyzerSynonymGraphFactory(Analyzer analyzerForParseSynonym){
        return new Factory("synonymgraph", analyzerForParseSynonym);
    }

    public class Factory implements TokenFilterFactory{

        private final String name;

        public Factory(String name, Analyzer analyzerForParseSynonym) {
            this.name = name;

            Analyzer analyzer;
            if (tokenizerFactory != null) {
                analyzer = new Analyzer() {
                    @Override
                    protected TokenStreamComponents createComponents(String fieldName) {
                        Tokenizer tokenizer = tokenizerFactory.create();
                        TokenStream stream = ignoreCase ? new LowerCaseFilter(tokenizer) : tokenizer;
                        return new TokenStreamComponents(tokenizer, stream);
                    }
                };
            } else {
                analyzer = analyzerForParseSynonym;
            }

            if (synonymMap == null) {
                try {
                    SynonymMap.Builder parser;
                    Reader rulesReader = new FastStringReader(rules);
                    if ("wordnet".equalsIgnoreCase(format)) {
                        parser = new WordnetSynonymParser(true, expand, analyzer);
                        ((WordnetSynonymParser) parser).parse(rulesReader);
                    } else {
                        parser = new SolrSynonymParser(true, expand, analyzer);
                        ((SolrSynonymParser) parser).parse(rulesReader);
                    }
                    synonymMap = parser.build();
                } catch (Exception e) {
                    throw new IllegalArgumentException("failed to build synonyms", e);
                }
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
