/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.compound.CompoundWordTokenFilterBase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.index.analysis.TokenFilterFactory;

/**
 * Contains the common configuration settings between subclasses of this class.
 */
public abstract class AbstractCompoundWordTokenFilterFactory extends AbstractTokenFilterFactory {

    protected final int minWordSize;
    protected final int minSubwordSize;
    protected final int maxSubwordSize;
    protected final boolean onlyLongestMatch;
    protected final CharArraySet wordList;
    // TODO expose this parameter?
    protected final boolean reuseChars;

    protected AbstractCompoundWordTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);

        minWordSize = settings.getAsInt("min_word_size", CompoundWordTokenFilterBase.DEFAULT_MIN_WORD_SIZE);
        minSubwordSize = settings.getAsInt("min_subword_size", CompoundWordTokenFilterBase.DEFAULT_MIN_SUBWORD_SIZE);
        maxSubwordSize = settings.getAsInt("max_subword_size", CompoundWordTokenFilterBase.DEFAULT_MAX_SUBWORD_SIZE);
        onlyLongestMatch = settings.getAsBoolean("only_longest_match", false);
        // TODO is the default of true correct? see: https://github.com/apache/lucene/pull/14278
        reuseChars = true;
        wordList = Analysis.getWordSet(env, settings, "word_list");
        if (wordList == null) {
            throw new IllegalArgumentException("word_list must be provided for [" + name + "], either as a path to a file, or directly");
        }
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        return IDENTITY_FILTER;     // don't decompound synonym file
    }
}
