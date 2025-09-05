/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.nori;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ko.KoreanTokenizer;
import org.apache.lucene.analysis.ko.dict.UserDictionary;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;
import org.elasticsearch.index.analysis.Analysis;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.IndexVersions.UPGRADE_LUCENE_9_9_1;

public class NoriTokenizerFactory extends AbstractTokenizerFactory {
    private static final String USER_DICT_PATH_OPTION = "user_dictionary";
    private static final String USER_DICT_RULES_OPTION = "user_dictionary_rules";
    private static final String LENIENT = "lenient";

    private final UserDictionary userDictionary;
    private final KoreanTokenizer.DecompoundMode decompoundMode;
    private final boolean discardPunctuation;

    public NoriTokenizerFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        decompoundMode = getMode(settings);
        userDictionary = getUserDictionary(env, settings, indexSettings);
        discardPunctuation = settings.getAsBoolean("discard_punctuation", true);
    }

    public static UserDictionary getUserDictionary(Environment env, Settings settings, IndexSettings indexSettings) {
        if (settings.get(USER_DICT_PATH_OPTION) != null && settings.get(USER_DICT_RULES_OPTION) != null) {
            throw new IllegalArgumentException(
                "It is not allowed to use [" + USER_DICT_PATH_OPTION + "] in conjunction" + " with [" + USER_DICT_RULES_OPTION + "]"
            );
        }
        List<String> ruleList = Analysis.getWordList(
            env,
            settings,
            USER_DICT_PATH_OPTION,
            USER_DICT_RULES_OPTION,
            LENIENT,
            false,  // typically don't want to remove comments as deduplication will provide better feedback
            isSupportDuplicateCheck(indexSettings)
        );
        if (ruleList == null || ruleList.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String line : ruleList) {
            sb.append(line).append(System.lineSeparator());
        }
        try (Reader rulesReader = new StringReader(sb.toString())) {
            return UserDictionary.open(rulesReader);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to load nori user dictionary", e);
        }
    }

    /**
     * Determines if the specified index version supports duplicate checks.
     * This method checks if the version of the index where it was created
     * is at Version 8.13.0 or above.
     * The feature of duplicate checks is introduced starting
     * from version 8.13.0, hence any versions earlier than this do not support duplicate checks.
     *
     * @param indexSettings The settings of the index in question.
     * @return Returns true if the version is 8.13.0 or later which means
     * that the duplicate check feature is supported.
     */
    private static boolean isSupportDuplicateCheck(IndexSettings indexSettings) {
        var idxVersion = indexSettings.getIndexVersionCreated();
        // Explicitly exclude the range of versions greater than NORI_DUPLICATES, that
        // are also in 8.12. The only version in this range is UPGRADE_LUCENE_9_9_1.
        return idxVersion.onOrAfter(IndexVersions.NORI_DUPLICATES) && idxVersion != UPGRADE_LUCENE_9_9_1;
    }

    public static KoreanTokenizer.DecompoundMode getMode(Settings settings) {
        String modeSetting = settings.get("decompound_mode", KoreanTokenizer.DEFAULT_DECOMPOUND.name());
        return KoreanTokenizer.DecompoundMode.valueOf(modeSetting.toUpperCase(Locale.ENGLISH));
    }

    @Override
    public Tokenizer create() {
        return new KoreanTokenizer(
            KoreanTokenizer.DEFAULT_TOKEN_ATTRIBUTE_FACTORY,
            userDictionary,
            decompoundMode,
            false,
            discardPunctuation
        );
    }

}
