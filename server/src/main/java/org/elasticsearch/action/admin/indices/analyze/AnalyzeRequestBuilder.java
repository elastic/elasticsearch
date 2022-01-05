/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

import java.util.Map;

public class AnalyzeRequestBuilder extends SingleShardOperationRequestBuilder<
    AnalyzeAction.Request,
    AnalyzeAction.Response,
    AnalyzeRequestBuilder> {

    public AnalyzeRequestBuilder(ElasticsearchClient client, AnalyzeAction action) {
        super(client, action, new AnalyzeAction.Request());
    }

    public AnalyzeRequestBuilder(ElasticsearchClient client, AnalyzeAction action, String index, String... text) {
        super(client, action, new AnalyzeAction.Request(index).text(text));
    }

    /**
     * Sets the analyzer name to use in order to analyze the text.
     *
     * @param analyzer The analyzer name.
     */
    public AnalyzeRequestBuilder setAnalyzer(String analyzer) {
        request.analyzer(analyzer);
        return this;
    }

    /**
     * Sets the field that its analyzer will be used to analyze the text. Note, requires an index
     * to be set.
     */
    public AnalyzeRequestBuilder setField(String field) {
        request.field(field);
        return this;
    }

    /**
     * Instead of setting the analyzer, sets the tokenizer as name that will be used as part of a custom
     * analyzer.
     */
    public AnalyzeRequestBuilder setTokenizer(String tokenizer) {
        request.tokenizer(tokenizer);
        return this;
    }

    /**
     * Instead of setting the analyzer, sets the tokenizer using custom settings that will be used as part of a custom
     * analyzer.
     */
    public AnalyzeRequestBuilder setTokenizer(Map<String, ?> tokenizer) {
        request.tokenizer(tokenizer);
        return this;
    }

    /**
     * Add token filter setting that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder addTokenFilter(Map<String, ?> tokenFilter) {
        request.addTokenFilter(tokenFilter);
        return this;
    }

    /**
     * Add a name of token filter that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder addTokenFilter(String tokenFilter) {
        request.addTokenFilter(tokenFilter);
        return this;
    }

    /**
     * Add char filter setting that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder addCharFilter(Map<String, ?> charFilter) {
        request.addCharFilter(charFilter);
        return this;
    }

    /**
     * Add a name of char filter that will be used before the tokenizer.
     */
    public AnalyzeRequestBuilder addCharFilter(String tokenFilter) {
        request.addCharFilter(tokenFilter);
        return this;
    }

    /**
     * Sets explain
     */
    public AnalyzeRequestBuilder setExplain(boolean explain) {
        request.explain(explain);
        return this;
    }

    /**
     * Sets attributes that will include results
     */
    public AnalyzeRequestBuilder setAttributes(String... attributes) {
        request.attributes(attributes);
        return this;
    }

    /**
     * Sets texts to analyze
     */
    public AnalyzeRequestBuilder setText(String... texts) {
        request.text(texts);
        return this;
    }

    /**
     * Instead of setting the analyzer and tokenizer, sets the normalizer as name
     */
    public AnalyzeRequestBuilder setNormalizer(String normalizer) {
        request.normalizer(normalizer);
        return this;
    }

}
