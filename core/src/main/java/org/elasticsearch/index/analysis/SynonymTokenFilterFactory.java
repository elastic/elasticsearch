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
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;

public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    private final SynonymMap synonymMap;
    private final boolean ignoreCase;

    public SynonymTokenFilterFactory(IndexSettings indexSettings, Environment env, Map<String, TokenizerFactory> tokenizerFactories,
                                      String name, Settings settings) throws IOException {
        super(indexSettings, name, settings);

        Reader rulesReader = null;
        if (settings.getAsArray("synonyms", null) != null) {
            List<String> rules = Analysis.getWordList(env, settings, "synonyms");
            StringBuilder sb = new StringBuilder();
            for (String line : rules) {
                sb.append(line).append(System.lineSeparator());
            }
            rulesReader = new FastStringReader(sb.toString());
        } else if (settings.get("synonyms_path") != null) {
            rulesReader = Analysis.getReaderFromFile(env, settings, "synonyms_path");
        } else {
            throw new IllegalArgumentException("synonym requires either `synonyms` or `synonyms_path` to be configured");
        }

        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        boolean expand = settings.getAsBoolean("expand", true);

        String tokenizerName = settings.get("tokenizer", "whitespace");
        final TokenizerFactory tokenizerFactory = tokenizerFactories.get(tokenizerName);
        if (tokenizerFactory == null) {
            throw new IllegalArgumentException("failed to find tokenizer [" + tokenizerName + "] for synonym token filter");
        }

        Analyzer analyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
                Tokenizer tokenizer = tokenizerFactory == null ? new WhitespaceTokenizer() : tokenizerFactory.create();
                TokenStream stream = ignoreCase ? new LowerCaseFilter(tokenizer) : tokenizer;
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        try {
            SynonymMap.Builder parser = null;

            if ("wordnet".equalsIgnoreCase(settings.get("format"))) {
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

    @Override
    public TokenStream create(TokenStream tokenStream) {
        // fst is null means no synonyms
        return synonymMap.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonymMap, ignoreCase);
    }
}
