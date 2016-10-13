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
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    protected final boolean ignoreCase;
    private final String format;
    private final boolean expand;
    private final String rules;
    protected SynonymMap synonymMap;

    public SynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, AnalysisRegistry analysisRegistry,
                                      String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);

        if (settings.getAsArray("synonyms", null) != null) {
            List<String> rulesList = Analysis.getWordList(env, settings, "synonyms");
            StringBuilder sb = new StringBuilder();
            for (String line : rulesList) {
                sb.append(line).append(System.lineSeparator());
            }
            rules = sb.toString();
        } else if (settings.get("synonyms_path") != null) {
            Reader rulesReader = Analysis.getReaderFromFile(env, settings, "synonyms_path");
            StringBuilder sb = new StringBuilder();
            String tmp;
            try (BufferedReader br = new BufferedReader(rulesReader)){
                while ((tmp = br.readLine()) != null) {
                    sb.append(tmp).append(System.getProperty("line.separator"));
                }
                rules = sb.toString();
            } catch (IOException ioe) {
                throw new IllegalArgumentException("failed to load synonyms", ioe);
            }

        } else {
            throw new IllegalArgumentException("synonym requires either `synonyms` or `synonyms_path` to be configured");
        }

        this.ignoreCase =
            settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "ignore_case", false, deprecationLogger);
        this.expand =
            settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "expand", true, deprecationLogger);

        // for backward compatibility
        String tokenizerName = settings.get("tokenizer", "");

        if (Strings.hasLength(tokenizerName)) {
            AnalysisModule.AnalysisProvider<TokenizerFactory> tokenizerFactoryFactory =
                analysisRegistry.getTokenizerProvider(tokenizerName, indexSettings);
            if (tokenizerFactoryFactory == null) {
                throw new IllegalArgumentException("failed to find tokenizer [" + tokenizerName + "] for synonym token filter");
            }
            final TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.get(indexSettings, env, tokenizerName,
                AnalysisRegistry.getSettingsFromIndexSettings(indexSettings,
                    AnalysisRegistry.INDEX_ANALYSIS_TOKENIZER + "." + tokenizerName));
            this.tokenizerFactory = tokenizerFactory;
        } else {
            this.tokenizerFactory = null;
        }

        this.format = settings.get("format", "");
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        // fst is null means no synonyms
        return synonymMap.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonymMap, ignoreCase);
    }

    public Factory createPerAnalyzerFactory(Analyzer analyzerForParseSynonym){
        return new Factory("synonym", analyzerForParseSynonym);
    }

    // for backward compatibility
    private final TokenizerFactory tokenizerFactory;

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
            return synonymMap.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonymMap, ignoreCase);
        }
    }

}
