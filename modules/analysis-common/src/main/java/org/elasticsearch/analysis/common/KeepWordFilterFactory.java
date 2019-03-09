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
        super(indexSettings, name, settings);

        final List<String> arrayKeepWords = settings.getAsList(KEEP_WORDS_KEY, null);
        final String keepWordsPath = settings.get(KEEP_WORDS_PATH_KEY, null);
        if ((arrayKeepWords == null && keepWordsPath == null) || (arrayKeepWords != null && keepWordsPath != null)) {
            // we don't allow both or none
            throw new IllegalArgumentException("keep requires either `" + KEEP_WORDS_KEY + "` or `"
                    + KEEP_WORDS_PATH_KEY + "` to be configured");
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
