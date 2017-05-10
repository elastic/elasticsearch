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

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ElasticsearchClient;

public class MultiSearchTemplateRequestBuilder
        extends ActionRequestBuilder<MultiSearchTemplateRequest, MultiSearchTemplateResponse, MultiSearchTemplateRequestBuilder> {

    protected MultiSearchTemplateRequestBuilder(ElasticsearchClient client, MultiSearchTemplateAction action) {
        super(client, action, new MultiSearchTemplateRequest());
    }

    public MultiSearchTemplateRequestBuilder add(SearchTemplateRequest request) {
        if (request.getRequest().indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed()
                && request().indicesOptions() != IndicesOptions.strictExpandOpenAndForbidClosed()) {
            request.getRequest().indicesOptions(request().indicesOptions());
        }

        super.request.add(request);
        return this;
    }

    public MultiSearchTemplateRequestBuilder add(SearchTemplateRequestBuilder request) {
        if (request.request().getRequest().indicesOptions() == IndicesOptions.strictExpandOpenAndForbidClosed()
                && request().indicesOptions() != IndicesOptions.strictExpandOpenAndForbidClosed()) {
            request.request().getRequest().indicesOptions(request().indicesOptions());
        }

        super.request.add(request);
        return this;
    }

    public MultiSearchTemplateRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request().indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Sets how many search requests specified in this multi search requests are allowed to be ran concurrently.
     */
    public MultiSearchTemplateRequestBuilder setMaxConcurrentSearchRequests(int maxConcurrentSearchRequests) {
        request().maxConcurrentSearchRequests(maxConcurrentSearchRequests);
        return this;
    }
}
