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

package org.elasticsearch.action.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.support.replication.ReplicationType;
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.util.Map;

/**
 * An index document action request builder.
 */
public class IndexRequestBuilder extends ShardReplicationOperationRequestBuilder<IndexRequest, IndexResponse, IndexRequestBuilder> {

    public IndexRequestBuilder(Client client) {
        super((InternalClient) client, new IndexRequest());
    }

    public IndexRequestBuilder(Client client, @Nullable String index) {
        super((InternalClient) client, new IndexRequest(index));
    }

    /**
     * Sets the type to index the document to.
     */
    public IndexRequestBuilder setType(String type) {
        request.setType(type);
        return this;
    }

    /**
     * Sets the id to index the document under. Optional, and if not set, one will be automatically
     * generated.
     */
    public IndexRequestBuilder setId(String id) {
        request.setId(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public IndexRequestBuilder setRouting(String routing) {
        request.setRouting(routing);
        return this;
    }

    /**
     * Sets the parent id of this document. If routing is not set, automatically set it as the
     * routing as well.
     */
    public IndexRequestBuilder setParent(String parent) {
        request.setParent(parent);
        return this;
    }

    /**
     * Sets the source.
     */
    public IndexRequestBuilder setSource(BytesReference source, boolean unsafe) {
        request.setSource(source, unsafe);
        return this;
    }

    /**
     * Sets the source.
     */
    public IndexRequestBuilder setSource(BytesReference source) {
        request.setSource(source, false);
        return this;
    }

    /**
     * Index the Map as a JSON.
     *
     * @param source The map to index
     */
    public IndexRequestBuilder setSource(Map<String, Object> source) {
        request.setSource(source);
        return this;
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public IndexRequestBuilder setSource(Map<String, Object> source, XContentType contentType) {
        request.setSource(source, contentType);
        return this;
    }

    /**
     * Sets the document source to index.
     * <p/>
     * <p>Note, its preferable to either set it using {@link #setSource(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #setSource(byte[])}.
     */
    public IndexRequestBuilder setSource(String source) {
        request.setSource(source);
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public IndexRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.setSource(sourceBuilder);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequestBuilder setSource(byte[] source) {
        request.setSource(source);
        return this;
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public IndexRequestBuilder setSource(byte[] source, int offset, int length) {
        request.setSource(source, offset, length);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     * @param unsafe Is the byte array safe to be used form a different thread
     */
    public IndexRequestBuilder setSource(byte[] source, int offset, int length, boolean unsafe) {
        request.setSource(source, offset, length, unsafe);
        return this;
    }

    /**
     * Constructs a simple document with a field and a value.
     */
    public IndexRequestBuilder setSource(String field1, Object value1) {
        request.setSource(field1, value1);
        return this;
    }

    /**
     * Constructs a simple document with a field and value pairs.
     */
    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2) {
        request.setSource(field1, value1, field2, value2);
        return this;
    }

    /**
     * Constructs a simple document with a field and value pairs.
     */
    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3) {
        request.setSource(field1, value1, field2, value2, field3, value3);
        return this;
    }

    /**
     * Constructs a simple document with a field and value pairs.
     */
    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3, String field4, Object value4) {
        request.setSource(field1, value1, field2, value2, field3, value3, field4, value4);
        return this;
    }

    /**
     * The content type that will be used to generate a document from user provided objects (like Map).
     */
    public IndexRequestBuilder setContentType(XContentType contentType) {
        request.setContentType(contentType);
        return this;
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequestBuilder setOpType(IndexRequest.OpType opType) {
        request.setOpType(opType);
        return this;
    }

    /**
     * Sets a string representation of the {@link #setOpType(org.elasticsearch.action.index.IndexRequest.OpType)}. Can
     * be either "index" or "create".
     */
    public IndexRequestBuilder setOpType(String opType) {
        request.setOpType(opType);
        return this;
    }

    /**
     * Set to <tt>true</tt> to force this index to use {@link org.elasticsearch.action.index.IndexRequest.OpType#CREATE}.
     */
    public IndexRequestBuilder setCreate(boolean create) {
        request.setCreate(create);
        return this;
    }

    /**
     * Should a refresh be executed post this index operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public IndexRequestBuilder setRefresh(boolean refresh) {
        request.setRefresh(refresh);
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    public IndexRequestBuilder setReplicationType(ReplicationType replicationType) {
        request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the consistency level. Defaults to {@link org.elasticsearch.action.WriteConsistencyLevel#DEFAULT}.
     */
    public IndexRequestBuilder setConsistencyLevel(WriteConsistencyLevel consistencyLevel) {
        request.consistencyLevel(consistencyLevel);
        return this;
    }

    /**
     * Set the replication type for this operation.
     */
    public IndexRequestBuilder setReplicationType(String replicationType) {
        request.replicationType(replicationType);
        return this;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public IndexRequestBuilder setVersion(long version) {
        request.setVersion(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    public IndexRequestBuilder setVersionType(VersionType versionType) {
        request.setVersionType(versionType);
        return this;
    }

    /**
     * Causes the index request document to be percolated. The parameter is the percolate query
     * to use to reduce the percolated queries that are going to run against this doc. Can be
     * set to <tt>*</tt> to indicate that all percolate queries should be run.
     */
    public IndexRequestBuilder setPercolate(String percolate) {
        request.setPercolate(percolate);
        return this;
    }

    /**
     * Sets the timestamp either as millis since the epoch, or, in the configured date format.
     */
    public IndexRequestBuilder setTimestamp(String timestamp) {
        request.setTimestamp(timestamp);
        return this;
    }

    // Sets the relative ttl value. It musts be > 0 as it makes little sense otherwise.
    public IndexRequestBuilder setTTL(long ttl) {
        request.setTtl(ttl);
        return this;
    }

    @Override
    protected void doExecute(ActionListener<IndexResponse> listener) {
        ((Client) client).index(request, listener);
    }
}
