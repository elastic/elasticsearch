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
import org.elasticsearch.Version;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    /**
     * @deprecated this property only works with tokenizer property
     */
    @Deprecated
    protected final boolean ignoreCase;
    protected final String format;
    protected final boolean expand;
    protected final Settings settings;

    public SynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, AnalysisRegistry analysisRegistry,
                                      String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);
        this.settings = settings;

        this.ignoreCase =
            settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "ignore_case", false, deprecationLogger);
        if (indexSettings.getIndexVersionCreated().onOrAfter(Version.V_6_0_0_beta1) && settings.get("ignore_case") != null) {
            deprecationLogger.deprecated(
                "This tokenize synonyms with whatever tokenizer and token filters appear before it in the chain. " +
                "If you need ignore case with this filter, you should set lowercase filter before this");
        }

        this.expand =
            settings.getAsBooleanLenientForPreEs6Indices(indexSettings.getIndexVersionCreated(), "expand", true, deprecationLogger);

        // for backward compatibility
        if (indexSettings.getIndexVersionCreated().before(Version.V_6_0_0_beta1)) {
            String tokenizerName = settings.get("tokenizer", "whitespace");
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
        throw new IllegalStateException("Call createPerAnalyzerSynonymFactory to specialize this factory for an analysis chain first");
    }

    protected Reader getRulesFromSettings(Environment env) {
        Reader rulesReader;
        if (settings.getAsList("synonyms", null) != null) {
            List<String> rulesList = Analysis.getWordList(env, settings, "synonyms");
            StringBuilder sb = new StringBuilder();
            for (String line : rulesList) {
                sb.append(line).append(System.lineSeparator());
            }
            rulesReader = new FastStringReader(sb.toString());
        } else if (settings.get("synonyms_path") != null) {
            rulesReader = Analysis.getReaderFromFile(env, settings, "synonyms_path");
        } else {
            throw new IllegalArgumentException("synonym requires either `synonyms` or `synonyms_path` to be configured");
        }
        return rulesReader;
    }

    Factory createPerAnalyzerSynonymFactory(Analyzer analyzerForParseSynonym, Environment env){
        return new Factory("synonym", analyzerForParseSynonym, getRulesFromSettings(env));
    }

    // for backward compatibility
    /**
     * @deprecated This filter tokenize synonyms with whatever tokenizer and token filters appear before it in the chain in 6.0.
     */
    @Deprecated
    protected final TokenizerFactory tokenizerFactory;

    public class Factory implements TokenFilterFactory{

        private final String name;
        private final SynonymMap synonymMap;

        public Factory(String name, Analyzer analyzerForParseSynonym, Reader rulesReader) {

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

            try {
                SynonymMap.Builder parser;
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
            } finally {
                if (tokenizerFactory != null) {
                    analyzer.close();
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
