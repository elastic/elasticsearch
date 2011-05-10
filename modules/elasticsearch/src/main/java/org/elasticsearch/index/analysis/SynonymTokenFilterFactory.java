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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@AnalysisSettingsRequired
public class SynonymTokenFilterFactory extends AbstractTokenFilterFactory {

    private final SynonymMap synonymMap;

    @Inject public SynonymTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, Environment env, Map<String, TokenizerFactoryFactory> tokenizerFactories,
                                             @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);

        List<String> rules = Analysis.getWordList(env, settings, "synonyms");
        if (rules == null) {
            throw new ElasticSearchIllegalArgumentException("synonym requires either `synonyms` or `synonyms_path` to be configured");
        }
        boolean ignoreCase = settings.getAsBoolean("ignore_case", false);
        boolean expand = settings.getAsBoolean("expand", true);

        TokenizerFactoryFactory tokenizerFactoryFactory = tokenizerFactories.get(settings.get("tokenizer", "whitespace"));
        TokenizerFactory tokenizerFactory = tokenizerFactoryFactory.create(settings.get("tokenizer", "whitespace"), settings);
        synonymMap = new SynonymMap(ignoreCase);
        parseRules(rules, synonymMap, "=>", ",", expand, tokenizerFactory);
    }

    @Override public TokenStream create(TokenStream tokenStream) {
        return new SynonymFilter(tokenStream, synonymMap);
    }

    static void parseRules(List<String> rules, SynonymMap map, String mappingSep,
                           String synSep, boolean expansion, TokenizerFactory tokFactory) {
        int count = 0;
        for (String rule : rules) {
            // To use regexes, we need an expression that specifies an odd number of chars.
            // This can't really be done with string.split(), and since we need to
            // do unescaping at some point anyway, we wouldn't be saving any effort
            // by using regexes.

            List<String> mapping = Strings.splitSmart(rule, mappingSep, false);

            List<List<String>> source;
            List<List<String>> target;

            if (mapping.size() > 2) {
                throw new RuntimeException("Invalid Synonym Rule:" + rule);
            } else if (mapping.size() == 2) {
                source = getSynList(mapping.get(0), synSep, tokFactory);
                target = getSynList(mapping.get(1), synSep, tokFactory);
            } else {
                source = getSynList(mapping.get(0), synSep, tokFactory);
                if (expansion) {
                    // expand to all arguments
                    target = source;
                } else {
                    // reduce to first argument
                    target = new ArrayList<List<String>>(1);
                    target.add(source.get(0));
                }
            }

            boolean includeOrig = false;
            for (List<String> fromToks : source) {
                count++;
                for (List<String> toToks : target) {
                    map.add(fromToks,
                            SynonymMap.makeTokens(toToks),
                            includeOrig,
                            true
                    );
                }
            }
        }
    }

    // a , b c , d e f => [[a],[b,c],[d,e,f]]
    private static List<List<String>> getSynList(String str, String separator, TokenizerFactory tokFactory) {
        List<String> strList = Strings.splitSmart(str, separator, false);
        // now split on whitespace to get a list of token strings
        List<List<String>> synList = new ArrayList<List<String>>();
        for (String toks : strList) {
            List<String> tokList = tokFactory == null ?
                    Strings.splitWS(toks, true) : splitByTokenizer(toks, tokFactory);
            synList.add(tokList);
        }
        return synList;
    }

    private static List<String> splitByTokenizer(String source, TokenizerFactory tokFactory) {
        TokenStream ts = tokFactory.create(new FastStringReader(source));
        List<String> tokList = new ArrayList<String>();
        try {
            CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            while (ts.incrementToken()) {
                if (termAtt.length() > 0)
                    tokList.add(termAtt.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tokList;
    }
}