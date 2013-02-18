/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.update;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

/**
 */
public class UpdateRequestBuilder extends InstanceShardOperationRequestBuilder<UpdateRequest, UpdateResponse, UpdateRequestBuilder> {

    public UpdateRequestBuilder(Client client) {
        super((InternalClient) client, new UpdateRequest());
    }

    public UpdateRequestBuilder(Client client, String index, String type, String id) {
        super((InternalClient) client, new UpdateRequest(index, type, id));
    }

    /**
     * Sets the type of the indexed document.
     */
    public UpdateRequestBuilder setType(String type) {
        request.setType(type);
        return this;
    }

    /**
     * Sets the id of the indexed document.
     */
    public UpdateRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public UpdateRequestBuilder setRouting(String routing) {
        request.setRouting(routing);
        return this;
    }

    public UpdateRequestBuilder setParent(String parent) {
        request.setParent(parent);
        return this;
    }

    /**
     * The script to execute. Note, make sure not to send different script each times and instead
     * use script params if possible with the same (automatically compiled) script.
     */
    public UpdateRequestBuilder setScript(String script) {
        request.setScript(script);
        return this;
    }

    /**
     * The language of the script to execute.
     */
    public UpdateRequestBuilder setScriptLang(String scriptLang) {
        request.setScriptLang(scriptLang);
        return this;
    }

    /**
     * Sets the script parameters to use with the script.
     */
    public UpdateRequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        request.setScriptParams(scriptParams);
        return this;
    }

    /**
     * Add a script parameter.
     */
    public UpdateRequestBuilder addScriptParam(String name, Object value) {
        request.addScriptParam(name, value);
        return this;
    }

    /**
     * Explicitly specify the fields that will be returned. By default, nothing is returned.
     */
    public UpdateRequestBuilder setFields(String... fields) {
        request.setFields(fields);
        return this;
    }

    /**
     * Sets the number of retries of a version conflict occurs because the document was updated between
     * getting it and updating it. Defaults to 1.
     */
    public UpdateRequestBuilder setRetryOnConflict(int retryOnConflict) {
        request.setRetryOnConflict(retryOnConflict);
        return this;
    }

    /**
     * Should a refresh be executed post this update operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public UpdateRequestBuilder setRefresh(boolean refresh) {
        request.setRefresh(refresh);
        return this;
    }

    /**
     * Sets the replication type.
     */
    public UpdateRequestBuilder setReplicationType(ReplicationType replicationType) {
        request.setReplicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level of write. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}
     */
    public UpdateRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.setConsistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * Causes the updated document to be percolated. The parameter is the percolate query
     * to use to reduce the percolated queries that are going to run against this doc. Can be
     * set to <tt>*</tt> to indicate that all percolate queries should be run.
     */
    public UpdateRequestBuilder setPercolate(String percolate) {
        request.setPercolate(percolate);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(IndexRequest indexRequest) {
        request.setDoc(indexRequest);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(XContentBuilder source) {
        request.setDoc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(Map source) {
        request.setDoc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(Map source, XContentType contentType) {
        request.setDoc(source, contentType);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(String source) {
        request.setDoc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(byte[] source) {
        request.setDoc(source);
        return this;
    }

    /**
     * Sets the doc to use for updates when a script is not specified.
     */
    public UpdateRequestBuilder setDoc(byte[] source, int offset, int length) {
        request.setDoc(source, offset, length);
        return this;
    }

    /**
     * Sets the index request to be used if the document does not exists. Otherwise, a {@link org.elasticsearch.index.engine.DocumentMissingException}
     * is thrown.
     */
    public UpdateRequestBuilder setUpsertRequest(IndexRequest indexRequest) {
        request.setUpsertRequest(indexRequest);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsertRequest(XContentBuilder source) {
        request.setUpsertRequest(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsertRequest(Map source) {
        request.setUpsertRequest(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsertRequest(Map source, XContentType contentType) {
        request.setUpsertRequest(source, contentType);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsertRequest(String source) {
        request.setUpsertRequest(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsertRequest(byte[] source) {
        request.setUpsertRequest(source);
        return this;
    }

    /**
     * Sets the doc source of the update request to be used when the document does not exists.
     */
    public UpdateRequestBuilder setUpsertRequest(byte[] source, int offset, int length) {
        request.setUpsertRequest(source, offset, length);
        return this;
    }

    public UpdateRequestBuilder setSource(XContentBuilder source) throws Exception {
        request.setSource(source);
        return this;
    }

    public UpdateRequestBuilder setSource(byte[] source) throws Exception {
        request.setSource(source);
        return this;
    }

    public UpdateRequestBuilder setSource(byte[] source, int offset, int length) throws Exception {
        request.setSource(source, offset, length);
        return this;
    }

    public UpdateRequestBuilder setSource(BytesReference source) throws Exception {
        request.setSource(source);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<UpdateResponse> listener) {
        ((Client) client).update(request, listener);
    }
}
