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

package org.elasticsearch.action.termvector;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

/**
 */
public class TermVectorRequestBuilder extends ActionRequestBuilder<TermVectorRequest, TermVectorResponse, TermVectorRequestBuilder> {

    public TermVectorRequestBuilder(Client client) {
        super((InternalClient) client, new TermVectorRequest());
    }

    public TermVectorRequestBuilder(Client client, String index, String type, String id) {
        super((InternalClient) client, new TermVectorRequest(index, type, id));
    }

    /**
     * Sets the routing. Required if routing isn't id based.
     */
    public TermVectorRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }
    
    /**
     * Sets the parent id of this document. Will simply set the routing to this value, as it is only
     * used for routing with delete requests.
     */
    public TermVectorRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    
    public TermVectorRequestBuilder setPreference(String preference) {
        request.preference(preference);
        return this;
    }
    
    public TermVectorRequestBuilder setOffsets(boolean offsets) {
        request.offsets(offsets);
        return this;
    }

    public TermVectorRequestBuilder setPositions(boolean positions) {
        request.positions(positions);
        return this;
    }

    public TermVectorRequestBuilder setPayloads(boolean payloads) {
        request.payloads(payloads);
        return this;
    }

    public TermVectorRequestBuilder setTermStatistics(boolean termStatistics) {
        request.termStatistics(termStatistics);
        return this;
    }

    public TermVectorRequestBuilder setFieldStatistics(boolean fieldStatistics) {
        request.fieldStatistics(fieldStatistics);
        return this;
    }

    public TermVectorRequestBuilder setSelectedFields(String... fields) {
        request.selectedFields(fields);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<TermVectorResponse> listener) {
        ((Client) client).termVector(request, listener);
    }
}
