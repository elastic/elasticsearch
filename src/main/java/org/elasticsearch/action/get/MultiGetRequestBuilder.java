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

package org.elasticsearch.action.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;

/**
 * A multi get document action request builder.
 */
public class MultiGetRequestBuilder extends ActionRequestBuilder<MultiGetRequest, MultiGetResponse, MultiGetRequestBuilder, Client> {

    public MultiGetRequestBuilder(Client client) {
        super(client, new MultiGetRequest());
    }

    public MultiGetRequestBuilder add(String index, @Nullable String type, String id) {
        request.add(index, type, id);
        return this;
    }

    public MultiGetRequestBuilder add(String index, @Nullable String type, Iterable<String> ids) {
        for (String id : ids) {
            request.add(index, type, id);
        }
        return this;
    }

    public MultiGetRequestBuilder add(String index, @Nullable String type, String... ids) {
        for (String id : ids) {
            request.add(index, type, id);
        }
        return this;
    }

    public MultiGetRequestBuilder add(MultiGetRequest.Item item) {
        request.add(item);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public MultiGetRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }

    /**
     * Should a refresh be executed before this get operation causing the operation to
     * return the latest value. Note, heavy get should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public MultiGetRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    public MultiGetRequestBuilder setRealtime(Boolean realtime) {
        request.realtime(realtime);
        return this;
    }

    public MultiGetRequestBuilder setIgnoreErrorsOnGeneratedFields(boolean ignoreErrorsOnGeneratedFields) {
        request.ignoreErrorsOnGeneratedFields(ignoreErrorsOnGeneratedFields);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<MultiGetResponse> listener) {
        client.multiGet(request, listener);
    }
}
