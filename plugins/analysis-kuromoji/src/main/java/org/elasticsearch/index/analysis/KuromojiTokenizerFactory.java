/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.apache.lucene.analysis.ja.util.CSVUtil;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KuromojiTokenizerFactory extends AbstractTokenizerFactory {

    private static final String USER_DICT_PATH_OPTION = "user_dictionary";
    private static final String USER_DICT_RULES_OPTION = "user_dictionary_rules";
    private static final String NBEST_COST = "nbest_cost";
    private static final String NBEST_EXAMPLES = "nbest_examples";
    private static final String DISCARD_COMPOUND_TOKEN = "discard_compound_token";

    private final UserDictionary userDictionary;
    private final Mode mode;
    private final String nBestExamples;
    private final int nBestCost;

    private boolean discardPunctuation;
    private boolean discardCompoundToken;

    public KuromojiTokenizerFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, settings, name);
        mode = getMode(settings);
        userDictionary = getUserDictionary(env, settings);
        discardPunctuation = settings.getAsBoolean("discard_punctuation", true);
        nBestCost = settings.getAsInt(NBEST_COST, -1);
        nBestExamples = settings.get(NBEST_EXAMPLES);
        discardCompoundToken = settings.getAsBoolean(DISCARD_COMPOUND_TOKEN, false);
    }

    public static UserDictionary getUserDictionary(Environment env, Settings settings) {
        if (settings.get(USER_DICT_PATH_OPTION) != null && settings.get(USER_DICT_RULES_OPTION) != null) {
            throw new IllegalArgumentException("It is not allowed to use [" + USER_DICT_PATH_OPTION + "] in conjunction" +
                " with [" + USER_DICT_RULES_OPTION + "]");
        }
        try {
            List<String> ruleList = Analysis.getWordList(env, settings, USER_DICT_PATH_OPTION, USER_DICT_RULES_OPTION, false);
            if (ruleList == null || ruleList.isEmpty()) {
                return null;
            }
            Set<String> dup = new HashSet<>();
            int lineNum = 0;
            for (String line : ruleList) {
                // ignore comments
                if (line.startsWith("#") == false) {
                    String[] values = CSVUtil.parse(line);
                    if (dup.add(values[0]) == false) {
                        throw new IllegalArgumentException("Found duplicate term [" + values[0] + "] in user dictionary " +
                            "at line [" + lineNum + "]");
                    }
                }
                ++ lineNum;
            }
            StringBuilder sb = new StringBuilder();
            for (String line : ruleList) {
                sb.append(line).append(System.lineSeparator());
            }
            return UserDictionary.open(new StringReader(sb.toString()));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load kuromoji user dictionary", e);
        }
    }

    public static JapaneseTokenizer.Mode getMode(Settings settings) {
        JapaneseTokenizer.Mode mode = JapaneseTokenizer.DEFAULT_MODE;
        String modeSetting = settings.get("mode", null);
        if (modeSetting != null) {
            if ("search".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseTokenizer.Mode.SEARCH;
            } else if ("normal".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseTokenizer.Mode.NORMAL;
            } else if ("extended".equalsIgnoreCase(modeSetting)) {
                mode = JapaneseTokenizer.Mode.EXTENDED;
            }
        }
        return mode;
    }

    @Override
    public Tokenizer create() {
        JapaneseTokenizer t = new JapaneseTokenizer(userDictionary, discardPunctuation, discardCompoundToken, mode);
        int nBestCost = this.nBestCost;
        if (nBestExamples != null) {
            nBestCost = Math.max(nBestCost, t.calcNBestCost(nBestExamples));
        }
        t.setNBestCost(nBestCost);
        return t;
    }

}
