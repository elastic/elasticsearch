/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.KeepWordFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.StopTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

import java.util.List;

/**
 * A {@link TokenFilterFactory} for {@link KeepWordFilter}. This filter only
 * keep tokens that are contained in the term set configured via
 * {@value #KEEP_WORDS_KEY} setting. This filter acts like an inverse stop
 * filter.
 * <p>
 * Configuration options:
 * <ul>
 * <li>{@value #KEEP_WORDS_KEY} the array of words / tokens to keep.</li>
 * <li>{@value #KEEP_WORDS_PATH_KEY} an reference to a file containing the words
 * / tokens to keep. Note: this is an alternative to {@value #KEEP_WORDS_KEY} if
 * both are set an exception will be thrown.</li>
 * <li>{@value #KEEP_WORDS_CASE_KEY} to use case sensitive keep words. The
 * default is <code>false</code> which corresponds to case-sensitive.</li>
 * </ul>
 *
 * @see StopTokenFilterFactory
 */
public class KeepWordFilterFactory extends AbstractTokenFilterFactory {
    private final CharArraySet keepWords;
    private static final String KEEP_WORDS_KEY = "keep_words";
    private static final String KEEP_WORDS_PATH_KEY = KEEP_WORDS_KEY + "_path";
    @SuppressWarnings("unused")
    private static final String KEEP_WORDS_CASE_KEY = KEEP_WORDS_KEY + "_case"; // for javadoc

    // unsupported ancient option
    private static final String ENABLE_POS_INC_KEY = "enable_position_increments";

    KeepWordFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name, settings);

        final List<String> arrayKeepWords = settings.getAsList(KEEP_WORDS_KEY, null);
        final String keepWordsPath = settings.get(KEEP_WORDS_PATH_KEY, null);
        if ((arrayKeepWords == null && keepWordsPath == null) || (arrayKeepWords != null && keepWordsPath != null)) {
            // we don't allow both or none
            throw new IllegalArgumentException(
                "keep requires either `" + KEEP_WORDS_KEY + "` or `" + KEEP_WORDS_PATH_KEY + "` to be configured"
            );
        }
        if (settings.get(ENABLE_POS_INC_KEY) != null) {
            throw new IllegalArgumentException(ENABLE_POS_INC_KEY + " is not supported anymore. Please fix your analysis chain");
        }
        this.keepWords = Analysis.getWordSet(env, settings, KEEP_WORDS_KEY);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new KeepWordFilter(tokenStream, keepWords);
    }
}
