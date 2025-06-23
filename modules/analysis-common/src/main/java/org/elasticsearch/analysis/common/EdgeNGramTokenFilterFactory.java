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
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;

public class EdgeNGramTokenFilterFactory extends AbstractTokenFilterFactory {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(EdgeNGramTokenFilterFactory.class);

    private final int minGram;

    private final int maxGram;

    public static final int SIDE_FRONT = 1;
    public static final int SIDE_BACK = 2;
    private final int side;
    private final boolean preserveOriginal;
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    EdgeNGramTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name);
        this.minGram = settings.getAsInt("min_gram", 1);
        this.maxGram = settings.getAsInt("max_gram", 2);
        if (settings.get("side") != null) {
            deprecationLogger.critical(
                DeprecationCategory.ANALYSIS,
                "edge_ngram_side_deprecated",
                "The [side] parameter is deprecated and will be removed. Use a [reverse] before and after the [edge_ngram] instead."
            );
        }
        this.side = parseSide(settings.get("side", "front"));
        this.preserveOriginal = settings.getAsBoolean(PRESERVE_ORIG_KEY, false);
    }

    static int parseSide(String side) {
        return switch (side) {
            case "front" -> SIDE_FRONT;
            case "back" -> SIDE_BACK;
            default -> throw new IllegalArgumentException("invalid side: " + side);
        };
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        TokenStream result = tokenStream;

        // side=BACK is not supported anymore but applying ReverseStringFilter up-front and after the token filter has the same effect
        if (side == SIDE_BACK) {
            result = new ReverseStringFilter(result);
        }

        result = new EdgeNGramTokenFilter(result, minGram, maxGram, preserveOriginal);

        // side=BACK is not supported anymore but applying ReverseStringFilter up-front and after the token filter has the same effect
        if (side == SIDE_BACK) {
            result = new ReverseStringFilter(result);
        }

        return result;
    }

    @Override
    public boolean breaksFastVectorHighlighter() {
        return true;
    }

    @Override
    public TokenFilterFactory getSynonymFilter() {
        throw new IllegalArgumentException("Token filter [" + name() + "] cannot be used to parse synonyms");
    }
}
