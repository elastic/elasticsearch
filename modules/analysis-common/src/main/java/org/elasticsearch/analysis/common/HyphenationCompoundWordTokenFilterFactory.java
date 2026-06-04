/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.compound.HyphenationCompoundWordTokenFilter;
import org.apache.lucene.analysis.compound.hyphenation.HyphenationTree;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.Analysis;
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

    private final boolean noSubMatches;
    private final boolean noOverlappingMatches;
    private final HyphenationTree hyphenationTree;
    private final Object sharingKey;

    HyphenationCompoundWordTokenFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, env, name, settings);

        String hyphenationPatternsPath = settings.get("hyphenation_patterns_path", null);
        if (hyphenationPatternsPath == null) {
            throw new IllegalArgumentException("hyphenation_patterns_path is a required setting.");
        }

        Path hyphenationPatternsFile = env.configDir().resolve(hyphenationPatternsPath);

        try {
            InputStream in = Files.newInputStream(hyphenationPatternsFile);
            hyphenationTree = HyphenationCompoundWordTokenFilter.getHyphenationTree(new InputSource(in));
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception while reading hyphenation_patterns_path.", e);
        }

        noSubMatches = settings.getAsBoolean("no_sub_matches", false);
        noOverlappingMatches = settings.getAsBoolean("no_overlapping_matches", false);
        // Capture a file stamp at construction so the cache invalidates if the hyphenation file
        // changes between index opens. HyphenationTree itself has no stable equality.
        Object fileStamp;
        try {
            fileStamp = new FileStamp(
                hyphenationPatternsPath,
                Files.getLastModifiedTime(hyphenationPatternsFile).toMillis(),
                Files.size(hyphenationPatternsFile)
            );
        } catch (Exception e) {
            fileStamp = new FileStamp(hyphenationPatternsPath, -1, -1);
        }
        this.sharingKey = new Key(
            minWordSize,
            minSubwordSize,
            maxSubwordSize,
            onlyLongestMatch,
            wordListKey,
            noSubMatches,
            noOverlappingMatches,
            fileStamp
        );
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
            onlyLongestMatch,
            noSubMatches,
            noOverlappingMatches
        );
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record FileStamp(String path, long mtime, long size) {}

    private record Key(
        int minWordSize,
        int minSubwordSize,
        int maxSubwordSize,
        boolean onlyLongestMatch,
        Analysis.StableCharArraySet wordList,
        boolean noSubMatches,
        boolean noOverlappingMatches,
        Object hyphenationFile
    ) {}
}
