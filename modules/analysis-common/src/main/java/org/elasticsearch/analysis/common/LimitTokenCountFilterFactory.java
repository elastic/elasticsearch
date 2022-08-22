/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

public class LimitTokenCountFilterFactory extends AbstractTokenFilterFactory {

    static final int DEFAULT_MAX_TOKEN_COUNT = 1;
    static final boolean DEFAULT_CONSUME_ALL_TOKENS = false;

    private final int maxTokenCount;
    private final boolean consumeAllTokens;

    LimitTokenCountFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name, settings);
        this.maxTokenCount = settings.getAsInt("max_token_count", DEFAULT_MAX_TOKEN_COUNT);
        this.consumeAllTokens = settings.getAsBoolean("consume_all_tokens", DEFAULT_CONSUME_ALL_TOKENS);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new LimitTokenCountFilter(tokenStream, maxTokenCount, consumeAllTokens);
    }
}
