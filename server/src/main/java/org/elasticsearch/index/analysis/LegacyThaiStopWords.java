/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.WordlistLoader;
import org.elasticsearch.index.IndexVersions;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/**
 * The Lucene 10.5 default Thai stopword set (115 entries). Lucene 10.6 (apache/lucene#16718) curated the list down to 87.
 * Indices created before {@link IndexVersions#THAI_ANALYSIS_LUCENE_10_6} must keep resolving {@code _thai_} and the
 * {@code thai} analyzer default to this set, otherwise terms that were indexed on the old set would no longer be
 * removed at query time (and vice versa for documents indexed after an upgrade).
 * <p>
 * The set lives in {@code server} rather than {@code analysis-common} so that {@link Analysis} can resolve
 * {@code _thai_} for old indices without a cross-module hook. It is loaded on first access of this class; the
 * backing resource ({@code legacy_thai_stopwords.txt}) is a verbatim copy of Lucene's file and must never be edited.
 */
public final class LegacyThaiStopWords {

    private static final String RESOURCE = "legacy_thai_stopwords.txt";

    /**
     * The Lucene 10.5 default Thai stop set. Loaded exactly as Lucene 10.5's {@code ThaiAnalyzer} loaded its file
     * (UTF-8, case-sensitive, {@code #} comments), then made unmodifiable since it is shared node-wide.
     */
    public static final CharArraySet DEFAULT_STOP_SET;

    static {
        InputStream stream = LegacyThaiStopWords.class.getResourceAsStream(RESOURCE);
        if (stream == null) {
            throw new IllegalStateException("Missing legacy Thai stopword resource [" + RESOURCE + "]");
        }
        try {
            // getWordSet closes the stream
            DEFAULT_STOP_SET = CharArraySet.unmodifiableSet(WordlistLoader.getWordSet(stream, "#"));
        } catch (IOException ex) {
            throw new UncheckedIOException("Unable to load legacy Thai stopword set", ex);
        }
    }

    private LegacyThaiStopWords() {}
}
