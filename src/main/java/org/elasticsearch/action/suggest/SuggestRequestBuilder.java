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

package org.elasticsearch.action.suggest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilder.SuggestionBuilder;

import java.io.IOException;

/**
 * A suggest action request builder.
 */
public class SuggestRequestBuilder extends BroadcastOperationRequestBuilder<SuggestRequest, SuggestResponse, SuggestRequestBuilder> {

    final SuggestBuilder suggest = new SuggestBuilder();

    public SuggestRequestBuilder(ElasticsearchClient client, SuggestAction action) {
        super(client, action, new SuggestRequest());
    }

    /**
     * Add a definition for suggestions to the request
     */
    public <T> SuggestRequestBuilder addSuggestion(SuggestionBuilder<T> suggestion) {
        suggest.addSuggestion(suggestion);
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SuggestRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    public SuggestRequestBuilder setSuggestText(String globalText) {
        this.suggest.setText(globalText);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards,
     * _shards:x,y to operate on shards x & y, or a custom value, which guarantees that the same order
     * will be used across different requests.
     */
    public SuggestRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public SuggestRequestBuilder setRouting(String... routing) {
        request.routing(routing);
        return this;
    }

    @Override
    protected SuggestRequest beforeExecute(SuggestRequest request) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            suggest.toXContent(builder, ToXContent.EMPTY_PARAMS);
            request.suggest(builder.bytes());
        } catch (IOException e) {
            throw new ElasticsearchException("Unable to build suggestion request", e);
        }
        return request;
    }
}
