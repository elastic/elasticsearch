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
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Map;

/**
 * The builder class for a term vector request.
 * Returns the term vector (doc frequency, positions, offsets) for a document.
 * <p/>
 * Note, the {@code index}, {@code type} and {@code id} are
 * required.
 */
public class TermVectorRequestBuilder extends ActionRequestBuilder<TermVectorRequest, TermVectorResponse, TermVectorRequestBuilder, Client> {

    public TermVectorRequestBuilder(Client client) {
        super(client, new TermVectorRequest());
    }

    /**
     * Constructs a new term vector request builder for a document that will be fetch
     * from the provided index. Use {@code index}, {@code type} and
     * {@code id} to specify the document to load.
     */
    public TermVectorRequestBuilder(Client client, String index, String type, String id) {
        super(client, new TermVectorRequest(index, type, id));
    }

    /**
     * Sets the index where the document is located.
     */
    public TermVectorRequestBuilder setIndex(String index) {
        request.index(index);
        return this;
    }

    /**
     * Sets the type of the document.
     */
    public TermVectorRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the id of the document.
     */
    public TermVectorRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Sets the artificial document from which to generate term vectors.
     */
    public TermVectorRequestBuilder setDoc(XContentBuilder xContent) {
        request.doc(xContent);
        return this;
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

    /**
     * Sets whether to return the start and stop offsets for each term if they were stored or
     * skip offsets.
     */
    public TermVectorRequestBuilder setOffsets(boolean offsets) {
        request.offsets(offsets);
        return this;
    }


    /**
     * Sets whether to return the positions for each term if stored or skip.
     */
    public TermVectorRequestBuilder setPositions(boolean positions) {
        request.positions(positions);
        return this;
    }

    /**
     * Sets whether to return the payloads for each term or skip.
     */
    public TermVectorRequestBuilder setPayloads(boolean payloads) {
        request.payloads(payloads);
        return this;
    }

    /**
     * Sets whether to return the term statistics for each term in the shard or skip.
     */
    public TermVectorRequestBuilder setTermStatistics(boolean termStatistics) {
        request.termStatistics(termStatistics);
        return this;
    }

    /**
     * Sets whether to return the field statistics for each term in the shard or skip.
     */
    public TermVectorRequestBuilder setFieldStatistics(boolean fieldStatistics) {
        request.fieldStatistics(fieldStatistics);
        return this;
    }

    /**
     * Sets whether to use distributed frequencies instead of shard statistics.
     */
    public TermVectorRequestBuilder setDfs(boolean dfs) {
        request.dfs(dfs);
        return this;
    }

    /**
     * Sets whether to return only term vectors for special selected fields. Returns the term
     * vectors for all fields if selectedFields == null
     */
    public TermVectorRequestBuilder setSelectedFields(String... fields) {
        request.selectedFields(fields);
        return this;
    }

    /**
     * Sets whether term vectors are generated real-time.
     */
    public TermVectorRequestBuilder setRealtime(Boolean realtime) {
        request.realtime(realtime);
        return this;
    }

    /**
     * Sets the analyzer used at each field when generating term vectors.
     */
    public TermVectorRequestBuilder setPerFieldAnalyzer(Map<String, String> perFieldAnalyzer) {
        request.perFieldAnalyzer(perFieldAnalyzer);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<TermVectorResponse> listener) {
        client.termVector(request, listener);
    }
}
