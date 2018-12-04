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

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class NoriTokenizerFactory extends AbstractTokenizerFactory {
    private static final String USER_DICT_PATH_OPTION = "user_dictionary";
    private static final String USER_DICT_RULES_OPTION = "user_dictionary_rules";

    private final UserDictionary userDictionary;
    private final KoreanTokenizer.DecompoundMode decompoundMode;

    public NoriTokenizerFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, settings);
        decompoundMode = getMode(settings);
        userDictionary = getUserDictionary(env, settings);
    }

    public static UserDictionary getUserDictionary(Environment env, Settings settings) {
        List<String> ruleList = Analysis.getWordList(env, settings, USER_DICT_PATH_OPTION, USER_DICT_RULES_OPTION);
        StringBuilder sb = new StringBuilder();
        if (ruleList == null || ruleList.isEmpty()) {
            return null;
        }
        // check for duplicate terms
        Set<String> terms = new HashSet<>();
        for (String line : ruleList) {
            String[] split = line.split("\\s+");
            if (terms.add(split[0]) == false) {
                throw new IllegalArgumentException("Found duplicate term: [" + split[0] + "] in user dictionary. ");
            }
            sb.append(line).append(System.lineSeparator());
        }
        try (Reader rulesReader = new StringReader(sb.toString())) {
            return UserDictionary.open(rulesReader);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load nori user dictionary", e);
        }
    }

    public static KoreanTokenizer.DecompoundMode getMode(Settings settings) {
        KoreanTokenizer.DecompoundMode mode = KoreanTokenizer.DEFAULT_DECOMPOUND;
        String modeSetting = settings.get("decompound_mode", null);
        if (modeSetting != null) {
            mode = KoreanTokenizer.DecompoundMode.valueOf(modeSetting.toUpperCase(Locale.ENGLISH));
        }
        return mode;
    }

    @Override
    public Tokenizer create() {
        return new KoreanTokenizer(KoreanTokenizer.DEFAULT_TOKEN_ATTRIBUTE_FACTORY, userDictionary, decompoundMode, false);
    }

}
