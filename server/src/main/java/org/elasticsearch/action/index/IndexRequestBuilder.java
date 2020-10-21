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

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
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
public class IndexRequestBuilder extends ReplicationRequestBuilder<IndexRequest, IndexResponse, IndexRequestBuilder>
        implements WriteRequestBuilder<IndexRequestBuilder> {

    public IndexRequestBuilder(ElasticsearchClient client, IndexAction action) {
        super(client, action, new IndexRequest());
    }

    public IndexRequestBuilder(ElasticsearchClient client, IndexAction action, @Nullable String index) {
        super(client, action, new IndexRequest(index));
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
     * Sets the source.
     */
    public IndexRequestBuilder setSource(BytesReference source, XContentType xContentType) {
        request.source(source, xContentType);
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
     * <p>
     * Note, its preferable to either set it using {@link #setSource(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #setSource(byte[], XContentType)}.
     */
    public IndexRequestBuilder setSource(String source, XContentType xContentType) {
        request.source(source, xContentType);
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
    public IndexRequestBuilder setSource(byte[] source, XContentType xContentType) {
        request.source(source, xContentType);
        return this;
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     * @param xContentType The type/format of the source
     */
    public IndexRequestBuilder setSource(byte[] source, int offset, int length, XContentType xContentType) {
        request.source(source, offset, length, xContentType);
        return this;
    }

    /**
     * Constructs a simple document with a field name and value pairs.
     * <p>
     * <b>Note: the number of objects passed to this method must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public IndexRequestBuilder setSource(Object... source) {
        request.source(source);
        return this;
    }

    /**
     * Constructs a simple document with a field name and value pairs.
     * <p>
     * <b>Note: the number of objects passed as varargs to this method must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public IndexRequestBuilder setSource(XContentType xContentType, Object... source) {
        request.source(xContentType, source);
        return this;
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequestBuilder setOpType(DocWriteRequest.OpType opType) {
        request.opType(opType);
        return this;
    }

    /**
     * Set to {@code true} to force this index to use {@link org.elasticsearch.action.index.IndexRequest.OpType#CREATE}.
     */
    public IndexRequestBuilder setCreate(boolean create) {
        request.create(create);
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
     * only perform this indexing request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public IndexRequestBuilder setIfSeqNo(long seqNo) {
        request.setIfSeqNo(seqNo);
        return this;
    }

    /**
     * only perform this indexing request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public IndexRequestBuilder setIfPrimaryTerm(long term) {
        request.setIfPrimaryTerm(term);
        return this;
    }

    /**
     * Sets the ingest pipeline to be executed before indexing the document
     */
    public IndexRequestBuilder setPipeline(String pipeline) {
        request.setPipeline(pipeline);
        return this;
    }

    /**
     * Sets the require_alias flag
     */
    public IndexRequestBuilder setRequireAlias(boolean requireAlias) {
        request.setRequireAlias(requireAlias);
        return this;
    }
}
