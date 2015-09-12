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

package org.elasticsearch.action.index;

import org.elasticsearch.action.support.replication.ReplicationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.util.Map;

/**
 * An index document action request builder.
 */
public class IndexRequestBuilder extends ReplicationRequestBuilder<IndexRequest, IndexResponse, IndexRequestBuilder> {

    public IndexRequestBuilder(ElasticsearchClient client, IndexAction action) {
        super(client, action, new IndexRequest());
    }

    public IndexRequestBuilder(ElasticsearchClient client, IndexAction action, @Nullable String index) {
        super(client, action, new IndexRequest(index));
    }

    /**
     * Sets the type to index the document to.
     */
    public IndexRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the id to index the document under. Optional, and if not set, one will be automatically
     * generated.
     */
    public IndexRequestBuilder setId(String id) {
        request.id(id);
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public IndexRequestBuilder setRouting(String routing) {
        request.routing(routing);
        return this;
    }

    /**
     * Sets the parent id of this document. If routing is not set, automatically set it as the
     * routing as well.
     */
    public IndexRequestBuilder setParent(String parent) {
        request.parent(parent);
        return this;
    }

    /**
     * Sets the source.
     */
    public IndexRequestBuilder setSource(BytesReference source) {
        request.source(source);
        return this;
    }

    /**
     * Index the Map as a JSON.
     *
     * @param source The map to index
     */
    public IndexRequestBuilder setSource(Map<String, ?> source) {
        request.source(source);
        return this;
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public IndexRequestBuilder setSource(Map<String, ?> source, XContentType contentType) {
        request.source(source, contentType);
        return this;
    }

    /**
     * Sets the document source to index.
     * <p/>
     * <p>Note, its preferable to either set it using {@link #setSource(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #setSource(byte[])}.
     */
    public IndexRequestBuilder setSource(String source) {
        request.source(source);
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public IndexRequestBuilder setSource(XContentBuilder sourceBuilder) {
        request.source(sourceBuilder);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequestBuilder setSource(byte[] source) {
        request.source(source);
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
        request.source(source, offset, length);
        return this;
    }

    /**
     * Constructs a simple document with a field and a value.
     */
    public IndexRequestBuilder setSource(String field1, Object value1) {
        request.source(field1, value1);
        return this;
    }

    /**
     * Constructs a simple document with a field and value pairs.
     */
    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2) {
        request.source(field1, value1, field2, value2);
        return this;
    }

    /**
     * Constructs a simple document with a field and value pairs.
     */
    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3) {
        request.source(field1, value1, field2, value2, field3, value3);
        return this;
    }

    /**
     * Constructs a simple document with a field and value pairs.
     */
    public IndexRequestBuilder setSource(String field1, Object value1, String field2, Object value2, String field3, Object value3, String field4, Object value4) {
        request.source(field1, value1, field2, value2, field3, value3, field4, value4);
        return this;
    }

    /**
     * Constructs a simple document with a field name and value pairs.
     * <b>Note: the number of objects passed to this method must be an even number.</b>
     */
    public IndexRequestBuilder setSource(Object... source) {
        request.source(source);
        return this;
    }

    /**
     * The content type that will be used to generate a document from user provided objects (like Map).
     */
    public IndexRequestBuilder setContentType(XContentType contentType) {
        request.contentType(contentType);
        return this;
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequestBuilder setOpType(IndexRequest.OpType opType) {
        request.opType(opType);
        return this;
    }

    /**
     * Sets a string representation of the {@link #setOpType(org.elasticsearch.action.index.IndexRequest.OpType)}. Can
     * be either "index" or "create".
     */
    public IndexRequestBuilder setOpType(String opType) {
        request.opType(IndexRequest.OpType.fromString(opType));
        return this;
    }

    /**
     * Set to <tt>true</tt> to force this index to use {@link org.elasticsearch.action.index.IndexRequest.OpType#CREATE}.
     */
    public IndexRequestBuilder setCreate(boolean create) {
        request.create(create);
        return this;
    }

    /**
     * Should a refresh be executed post this index operation causing the operation to
     * be searchable. Note, heavy indexing should not set this to <tt>true</tt>. Defaults
     * to <tt>false</tt>.
     */
    public IndexRequestBuilder setRefresh(boolean refresh) {
        request.refresh(refresh);
        return this;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public IndexRequestBuilder setVersion(long version) {
        request.version(version);
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    public IndexRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    /**
     * Sets the timestamp either as millis since the epoch, or, in the configured date format.
     */
    public IndexRequestBuilder setTimestamp(String timestamp) {
        request.timestamp(timestamp);
        return this;
    }

    // Sets the relative ttl value. It musts be > 0 as it makes little sense otherwise.
    public IndexRequestBuilder setTTL(long ttl) {
        request.ttl(ttl);
        return this;
    }
}
