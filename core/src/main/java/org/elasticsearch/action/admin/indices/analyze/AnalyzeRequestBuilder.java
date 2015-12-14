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
package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.action.support.single.shard.SingleShardOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 *
 */
public class AnalyzeRequestBuilder extends SingleShardOperationRequestBuilder<AnalyzeRequest, AnalyzeResponse, AnalyzeRequestBuilder> {

    public AnalyzeRequestBuilder(ElasticsearchClient client, AnalyzeAction action) {
        super(client, action, new AnalyzeRequest());
    }

    public AnalyzeRequestBuilder(ElasticsearchClient client, AnalyzeAction action, String index, String... text) {
        super(client, action, new AnalyzeRequest(index).text(text));
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
     * Instead of setting the analyzer, sets the tokenizer that will be used as part of a custom
     * analyzer.
     */
    public AnalyzeRequestBuilder setTokenizer(String tokenizer) {
        request.tokenizer(tokenizer);
        return this;
    }

    /**
     * Sets token filters that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder setTokenFilters(String... tokenFilters) {
        request.tokenFilters(tokenFilters);
        return this;
    }

    /**
     * Sets char filters that will be used before the tokenizer.
     */
    public AnalyzeRequestBuilder setCharFilters(String... charFilters) {
        request.charFilters(charFilters);
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
    public AnalyzeRequestBuilder setAttributes(String attributes){
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
}
