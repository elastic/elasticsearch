/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer.Mode;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenizerFactory;
import org.elasticsearch.index.analysis.Analysis;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;

/**
 * Factory for creating Kuromoji tokenizers that perform Japanese morphological analysis.
 * Supports multiple segmentation modes, custom user dictionaries, and n-best tokenization.
 */
public class KuromojiTokenizerFactory extends AbstractTokenizerFactory {

    private static final String USER_DICT_PATH_OPTION = "user_dictionary";
    private static final String USER_DICT_RULES_OPTION = "user_dictionary_rules";
    private static final String NBEST_COST = "nbest_cost";
    private static final String NBEST_EXAMPLES = "nbest_examples";
    private static final String DISCARD_COMPOUND_TOKEN = "discard_compound_token";
    private static final String LENIENT = "lenient";

    private final UserDictionary userDictionary;
    private final Mode mode;
    private final String nBestExamples;
    private final int nBestCost;

    private boolean discardPunctuation;
    private boolean discardCompoundToken;

    /**
     * Constructs a Kuromoji tokenizer factory with configurable tokenization behavior.
     *
     * @param indexSettings the index settings
     * @param env the environment for resolving user dictionary files
     * @param name the tokenizer name
     * @param settings the tokenizer settings containing:
     *        <ul>
     *        <li>mode: tokenization mode - "normal", "search", or "extended" (default: JapaneseTokenizer.DEFAULT_MODE)</li>
     *        <li>user_dictionary: path to user dictionary file</li>
     *        <li>user_dictionary_rules: inline user dictionary rules (mutually exclusive with user_dictionary)</li>
     *        <li>discard_punctuation: whether to discard punctuation tokens (default: true)</li>
     *        <li>nbest_cost: cost threshold for n-best tokenization (default: -1, disabled)</li>
     *        <li>nbest_examples: example text for calculating n-best cost</li>
     *        <li>discard_compound_token: whether to discard compound tokens in search mode (default: false)</li>
     *        </ul>
     * @throws IllegalArgumentException if both user_dictionary and user_dictionary_rules are specified
     * @throws ElasticsearchException if the user dictionary cannot be loaded
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "tokenizer": {
     *   "my_kuromoji": {
     *     "type": "kuromoji_tokenizer",
     *     "mode": "search",
     *     "discard_punctuation": true,
     *     "user_dictionary_rules": ["東京スカイツリー,東京 スカイツリー,トウキョウ スカイツリー,カスタム名詞"]
     *   }
     * }
     * }</pre>
     */
    public KuromojiTokenizerFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        mode = getMode(settings);
        userDictionary = getUserDictionary(env, settings);
        discardPunctuation = settings.getAsBoolean("discard_punctuation", true);
        nBestCost = settings.getAsInt(NBEST_COST, -1);
        nBestExamples = settings.get(NBEST_EXAMPLES);
        discardCompoundToken = settings.getAsBoolean(DISCARD_COMPOUND_TOKEN, false);
    }

    /**
     * Loads a user dictionary from settings, either from a file path or inline rules.
     * User dictionaries allow customization of tokenization by defining custom entries.
     *
     * @param env the environment for resolving dictionary file paths
     * @param settings the settings containing user dictionary configuration
     * @return a {@link UserDictionary} if dictionary is configured, null otherwise
     * @throws IllegalArgumentException if both file path and inline rules are specified
     * @throws ElasticsearchException if the dictionary file cannot be loaded or parsed
     */
    public static UserDictionary getUserDictionary(Environment env, Settings settings) {
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
            true
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
            throw new ElasticsearchException("failed to load kuromoji user dictionary", e);
        }
    }

    /**
     * Extracts the tokenization mode from settings.
     * Modes control how text is segmented:
     * <ul>
     * <li>NORMAL: regular tokenization</li>
     * <li>SEARCH: additional sub-word segmentation for better search recall</li>
     * <li>EXTENDED: most aggressive segmentation</li>
     * </ul>
     *
     * @param settings the settings containing the mode parameter
     * @return the {@link JapaneseTokenizer.Mode} specified in settings, or the default mode
     */
    public static JapaneseTokenizer.Mode getMode(Settings settings) {
        String modeSetting = settings.get("mode", JapaneseTokenizer.DEFAULT_MODE.name());
        return JapaneseTokenizer.Mode.valueOf(modeSetting.toUpperCase(Locale.ENGLISH));
    }

    /**
     * Creates a new Kuromoji tokenizer with the configured settings.
     * The tokenizer applies user dictionary if configured and sets n-best cost based on
     * examples or explicit configuration.
     *
     * @return a new {@link JapaneseTokenizer} instance with the configured parameters
     */
    @Override
    public Tokenizer create() {
        JapaneseTokenizer t = new JapaneseTokenizer(userDictionary, discardPunctuation, discardCompoundToken, mode);
        int nBestCostValue = this.nBestCost;
        if (nBestExamples != null) {
            nBestCostValue = Math.max(nBestCostValue, t.calcNBestCost(nBestExamples));
        }
        t.setNBestCost(nBestCostValue);
        return t;
    }

}
