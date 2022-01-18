/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.xml.sax.InputSource;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Uses the {@link org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter} to decompound tokens based on hyphenation rules.
 *
 * @see org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter
 */
public class HyphenationCompoundWordTokenFilterFactory extends AbstractCompoundWordTokenFilterFactory {

    private final HyphenationTree hyphenationTree;

    HyphenationCompoundWordTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, env, name, settings);

        String hyphenationPatternsPath = settings.get("hyphenation_patterns_path", null);
        if (hyphenationPatternsPath == null) {
            throw new IllegalArgumentException("hyphenation_patterns_path is a required setting.");
        }

        Path hyphenationPatternsFile = env.configFile().resolve(hyphenationPatternsPath);

        try {
            InputStream in = Files.newInputStream(hyphenationPatternsFile);
            hyphenationTree = HyphenationCompoundWordTokenFilter.getHyphenationTree(new InputSource(in));
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception while reading hyphenation_patterns_path.", e);
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new HyphenationCompoundWordTokenFilter(
            tokenStream,
            hyphenationTree,
            wordList,
            minWordSize,
            minSubwordSize,
            maxSubwordSize,
            onlyLongestMatch
        );
    }
}
