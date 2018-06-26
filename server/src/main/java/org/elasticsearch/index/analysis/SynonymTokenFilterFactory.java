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
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;

public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    protected final String format;
    protected final boolean expand;
    protected final boolean lenient;
    protected final Settings settings;

    public SynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, AnalysisRegistry analysisRegistry,
                                      String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);
        this.settings = settings;

        if (settings.get("ignore_case") != null) {
            deprecationLogger.deprecated(
                "The ignore_case option on the synonym_graph filter is deprecated. " +
                    "Instead, insert a lowercase filter in the filter chain before the synonym_graph filter.");
        }

        this.expand = settings.getAsBoolean("expand", true);
        this.lenient = settings.getAsBoolean("lenient", false);
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
            rulesReader = new StringReader(sb.toString());
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

    public class Factory implements TokenFilterFactory{

        private final String name;
        private final SynonymMap synonymMap;

        public Factory(String name, Analyzer analyzerForParseSynonym, Reader rulesReader) {

            this.name = name;

            try {
                SynonymMap.Builder parser;
                if ("wordnet".equalsIgnoreCase(format)) {
                    parser = new ESWordnetSynonymParser(true, expand, lenient, analyzerForParseSynonym);
                    ((ESWordnetSynonymParser) parser).parse(rulesReader);
                } else {
                    parser = new ESSolrSynonymParser(true, expand, lenient, analyzerForParseSynonym);
                    ((ESSolrSynonymParser) parser).parse(rulesReader);
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
            return synonymMap.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonymMap, false);
        }
    }

}
