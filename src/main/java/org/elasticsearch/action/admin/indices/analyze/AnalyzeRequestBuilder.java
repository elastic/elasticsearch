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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.single.custom.SingleCustomOperationRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.bytes.BytesReference;

/**
 *
 */
public class AnalyzeRequestBuilder extends SingleCustomOperationRequestBuilder<AnalyzeRequest, AnalyzeResponse, AnalyzeRequestBuilder> {

    private AnalyzeSourceBuilder builder;

    public AnalyzeRequestBuilder(IndicesAdminClient indicesClient) {
        super(indicesClient, new AnalyzeRequest());
    }

    public AnalyzeRequestBuilder(IndicesAdminClient indicesClient, String index, String text) {
        super(indicesClient, new AnalyzeRequest(index));
        sourceBuilder().setText(text);
    }

    /**
     * Sets the index to use to analyzer the text (for example, if it holds specific analyzers
     * registered).
     */
    public AnalyzeRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the analyzer name to use in order to analyze the text.
     *
     * @param analyzer The analyzer name.
     */
    public AnalyzeRequestBuilder setAnalyzer(String analyzer) {
        sourceBuilder().setAnalyzer(analyzer);
        return this;
    }

    /**
     * Sets the field that its analyzer will be used to analyze the text. Note, requires an index
     * to be set.
     */
    public AnalyzeRequestBuilder setField(String field) {
        sourceBuilder().setField(field);
        return this;
    }

    /**
     * Instead of setting the analyzer, sets the tokenizer that will be used as part of a custom
     * analyzer.
     */
    public AnalyzeRequestBuilder setTokenizer(String tokenizer) {
        sourceBuilder().setTokenizer(tokenizer);
        return this;
    }

    /**
     * Sets token filters that will be used on top of a tokenizer provided.
     */
    public AnalyzeRequestBuilder setTokenFilters(String... tokenFilters) {
        sourceBuilder().setTokenFilters(tokenFilters);
        return this;
    }

    /**
     * Sets char filters that will be used before the tokenizer.
     */
    public AnalyzeRequestBuilder setCharFilters(String... charFilters) {
        sourceBuilder().setCharFilters(charFilters);
        return this;
    }

    public AnalyzeRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    public AnalyzeRequestBuilder setSource(BytesReference source, boolean unsafe) {
        request.source(source, unsafe);
        return this;
    }

    public AnalyzeRequestBuilder setSource(AnalyzeSourceBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    public AnalyzeRequestBuilder setText(String text) {
        sourceBuilder().setText(text);
        return this;
    }

    private AnalyzeSourceBuilder sourceBuilder() {
        if (builder == null) {
            builder = new AnalyzeSourceBuilder();
        }
        return builder;
    }

    @Override
    protected void doExecute(ActionListener<AnalyzeResponse> listener) {
        if (builder != null && request.source() == null) {
            request.source(builder);
        }
        client.analyze(request, listener);
    }
}
