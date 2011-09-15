/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AnalysisSettingsRequired
public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    private final SynonymMap synonymMap;
    private final boolean ignoreCase;

    @Inject public SynonymTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, Environment env, IndicesAnalysisService indicesAnalysisService, Map<String, TokenizerFactoryFactory> tokenizerFactories,
                                             @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);

        List<String> rules = Analysis.getWordList(env, settings, "synonyms");
        if (rules == null) {
            throw new ElasticSearchIllegalArgumentException("synonym requires either `synonyms` or `synonyms_path` to be configured");
        }
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        boolean expand = settings.getAsBoolean("expand", true);

        String tokenizerName = settings.get("tokenizer", "whitespace");

        TokenizerFactoryFactory tokenizerFactoryFactory = tokenizerFactories.get(tokenizerName);
        if (tokenizerFactoryFactory == null) {
            tokenizerFactoryFactory = indicesAnalysisService.tokenizerFactoryFactory(tokenizerName);
        }
        if (tokenizerFactoryFactory == null) {
            throw new ElasticSearchIllegalArgumentException("failed to fine tokenizer [" + tokenizerName + "] for synonym token filter");
        }
        final TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.create(tokenizerName, settings);

        Analyzer analyzer = new ReusableAnalyzerBase() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                Tokenizer tokenizer = tokenizerFactory == null ? new WhitespaceTokenizer(Lucene.ANALYZER_VERSION, reader) : tokenizerFactory.create(reader);
                TokenStream stream = ignoreCase ? new LowerCaseFilter(Lucene.ANALYZER_VERSION, tokenizer) : tokenizer;
                return new TokenStreamComponents(tokenizer, stream);
            }
        };

        CustomSynonymParser parser = new CustomSynonymParser(true, expand, analyzer);
        try {
            synonymMap = parser.build();
        } catch (IOException e) {
            throw new ElasticSearchIllegalArgumentException("failed to build synonyms", e);
        }
    }

    @Override public TokenStream create(TokenStream tokenStream) {
        // fst is null means no synonyms
        return synonymMap.fst == null ? tokenStream : new SynonymFilter(tokenStream, synonymMap, ignoreCase);
    }

    /**
     * Parser for the Solr synonyms format.
     * <ol>
     * <li> Blank lines and lines starting with '#' are comments.
     * <li> Explicit mappings match any token sequence on the LHS of "=>"
     * and replace with all alternatives on the RHS.  These types of mappings
     * ignore the expand parameter in the constructor.
     * Example:
     * <blockquote>i-pod, i pod => ipod</blockquote>
     * <li> Equivalent synonyms may be separated with commas and give
     * no explicit mapping.  In this case the mapping behavior will
     * be taken from the expand parameter in the constructor.  This allows
     * the same synonym file to be used in different synonym handling strategies.
     * Example:
     * <blockquote>ipod, i-pod, i pod</blockquote>
     *
     * <li> Multiple synonym mapping entries are merged.
     * Example:
     * <blockquote>
     * foo => foo bar<br>
     * foo => baz<br><br>
     * is equivalent to<br><br>
     * foo => foo bar, baz
     * </blockquote>
     * </ol>
     *
     * @lucene.experimental
     */
    public static class CustomSynonymParser extends SynonymMap.Builder {
        private final boolean expand;
        private final Analyzer analyzer;

        public CustomSynonymParser(boolean dedup, boolean expand, Analyzer analyzer) {
            super(dedup);
            this.expand = expand;
            this.analyzer = analyzer;
        }

        public void add(Reader in) throws IOException, ParseException {
            LineNumberReader br = new LineNumberReader(in);
            try {
                addInternal(br);
            } catch (IllegalArgumentException e) {
                ParseException ex = new ParseException("Invalid synonym rule at line " + br.getLineNumber(), 0);
                ex.initCause(e);
                throw ex;
            } finally {
                br.close();
            }
        }

        public void addLine(String line) throws IOException {
            if (line.length() == 0 || line.charAt(0) == '#') {
                return;
            }

            CharsRef inputs[];
            CharsRef outputs[];

            // TODO: we could process this more efficiently.
            String sides[] = split(line, "=>");
            if (sides.length > 1) { // explicit mapping
                if (sides.length != 2) {
                    throw new IllegalArgumentException("more than one explicit mapping specified on the same line");
                }
                String inputStrings[] = split(sides[0], ",");
                inputs = new CharsRef[inputStrings.length];
                for (int i = 0; i < inputs.length; i++) {
                    inputs[i] = analyze(analyzer, unescape(inputStrings[i]).trim(), new CharsRef());
                }

                String outputStrings[] = split(sides[1], ",");
                outputs = new CharsRef[outputStrings.length];
                for (int i = 0; i < outputs.length; i++) {
                    outputs[i] = analyze(analyzer, unescape(outputStrings[i]).trim(), new CharsRef());
                }
            } else {
                String inputStrings[] = split(line, ",");
                inputs = new CharsRef[inputStrings.length];
                for (int i = 0; i < inputs.length; i++) {
                    inputs[i] = analyze(analyzer, unescape(inputStrings[i]).trim(), new CharsRef());
                }
                if (expand) {
                    outputs = inputs;
                } else {
                    outputs = new CharsRef[1];
                    outputs[0] = inputs[0];
                }
            }

            // currently we include the term itself in the map,
            // and use includeOrig = false always.
            // this is how the existing filter does it, but its actually a bug,
            // especially if combined with ignoreCase = true
            for (int i = 0; i < inputs.length; i++) {
                for (int j = 0; j < outputs.length; j++) {
                    add(inputs[i], outputs[j], false);
                }
            }
        }

        private void addInternal(BufferedReader in) throws IOException {
            String line = null;
            while ((line = in.readLine()) != null) {
                addLine(line);
            }
        }

        private static String[] split(String s, String separator) {
            ArrayList<String> list = new ArrayList<String>(2);
            StringBuilder sb = new StringBuilder();
            int pos = 0, end = s.length();
            while (pos < end) {
                if (s.startsWith(separator, pos)) {
                    if (sb.length() > 0) {
                        list.add(sb.toString());
                        sb = new StringBuilder();
                    }
                    pos += separator.length();
                    continue;
                }

                char ch = s.charAt(pos++);
                if (ch == '\\') {
                    sb.append(ch);
                    if (pos >= end) break;  // ERROR, or let it go?
                    ch = s.charAt(pos++);
                }

                sb.append(ch);
            }

            if (sb.length() > 0) {
                list.add(sb.toString());
            }

            return list.toArray(new String[list.size()]);
        }

        private String unescape(String s) {
            if (s.indexOf("\\") >= 0) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < s.length(); i++) {
                    char ch = s.charAt(i);
                    if (ch == '\\' && i < s.length() - 1) {
                        sb.append(s.charAt(++i));
                    } else {
                        sb.append(ch);
                    }
                }
                return sb.toString();
            }
            return s;
        }
    }
}