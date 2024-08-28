/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 * An index document action request builder.
 */
public class IndexRequestBuilder extends ReplicationRequestBuilder<IndexRequest, DocWriteResponse, IndexRequestBuilder>
    implements
        WriteRequestBuilder<IndexRequestBuilder> {
    private String id = null;

    private BytesReference sourceBytesReference;
    private XContentType sourceContentType;

    private String pipeline;
    private Boolean requireAlias;
    private Boolean requireDataStream;
    private String routing;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private Long ifSeqNo;
    private Long ifPrimaryTerm;
    private DocWriteRequest.OpType opType;
    private Boolean create;
    private Long version;
    private VersionType versionType;

    public IndexRequestBuilder(ElasticsearchClient client) {
        this(client, null);
    }

    @SuppressWarnings("this-escape")
    public IndexRequestBuilder(ElasticsearchClient client, @Nullable String index) {
        super(client, TransportIndexAction.TYPE);
        setIndex(index);
    }

    /**
     * Sets the id to index the document under. Optional, and if not set, one will be automatically
     * generated.
     */
    public IndexRequestBuilder setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    public IndexRequestBuilder setRouting(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the source.
     */
    public IndexRequestBuilder setSource(BytesReference source, XContentType xContentType) {
        this.sourceBytesReference = source;
        this.sourceContentType = xContentType;
        return this;
    }

    /**
     * Index the Map as a JSON.
     *
     * @param source The map to index
     */
    public IndexRequestBuilder setSource(Map<String, ?> source) {
        return setSource(source, Requests.INDEX_CONTENT_TYPE);
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public IndexRequestBuilder setSource(Map<String, ?> source, XContentType contentType) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return setSource(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    /**
     * Sets the document source to index.
     * <p>
     * Note, its preferable to either set it using {@link #setSource(org.elasticsearch.xcontent.XContentBuilder)}
     * or using the {@link #setSource(byte[], XContentType)}.
     */
    public IndexRequestBuilder setSource(String source, XContentType xContentType) {
        this.sourceBytesReference = new BytesArray(source);
        this.sourceContentType = xContentType;
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public IndexRequestBuilder setSource(XContentBuilder sourceBuilder) {
        this.sourceBytesReference = BytesReference.bytes(sourceBuilder);
        this.sourceContentType = sourceBuilder.contentType();
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequestBuilder setSource(byte[] source, XContentType xContentType) {
        return setSource(source, 0, source.length, xContentType);
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
        this.sourceBytesReference = new BytesArray(source, offset, length);
        this.sourceContentType = xContentType;
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
        return setSource(Requests.INDEX_CONTENT_TYPE, source);
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
        return setSource(IndexRequest.getXContentBuilder(xContentType, source));
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequestBuilder setOpType(DocWriteRequest.OpType opType) {
        this.opType = opType;
        return this;
    }

    /**
     * Set to {@code true} to force this index to use {@link org.elasticsearch.action.index.IndexRequest.OpType#CREATE}.
     */
    public IndexRequestBuilder setCreate(boolean create) {
        this.create = create;
        return this;
    }

    /**
     * Sets the version, which will cause the index operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public IndexRequestBuilder setVersion(long version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the versioning type. Defaults to {@link VersionType#INTERNAL}.
     */
    public IndexRequestBuilder setVersionType(VersionType versionType) {
        this.versionType = versionType;
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
        this.ifSeqNo = seqNo;
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
        this.ifPrimaryTerm = term;
        return this;
    }

    /**
     * Sets the ingest pipeline to be executed before indexing the document
     */
    public IndexRequestBuilder setPipeline(String pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    /**
     * Sets the require_alias flag
     */
    public IndexRequestBuilder setRequireAlias(boolean requireAlias) {
        this.requireAlias = requireAlias;
        return this;
    }

    /**
     * Sets the require_data_stream flag
     */
    public IndexRequestBuilder setRequireDataStream(boolean requireDataStream) {
        this.requireDataStream = requireDataStream;
        return this;
    }

    public IndexRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    public IndexRequestBuilder setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(refreshPolicy);
        return this;
    }

    @Override
    public IndexRequest request() {
        IndexRequest request = new IndexRequest();
        super.apply(request);
        request.id(id);
        if (sourceBytesReference != null && sourceContentType != null) {
            request.source(sourceBytesReference, sourceContentType);
        }
        if (pipeline != null) {
            request.setPipeline(pipeline);
        }
        if (routing != null) {
            request.routing(routing);
        }
        if (refreshPolicy != null) {
            request.setRefreshPolicy(refreshPolicy);
        }
        if (ifSeqNo != null) {
            request.setIfSeqNo(ifSeqNo);
        }
        if (ifPrimaryTerm != null) {
            request.setIfPrimaryTerm(ifPrimaryTerm);
        }
        if (pipeline != null) {
            request.setPipeline(pipeline);
        }
        if (requireAlias != null) {
            request.setRequireAlias(requireAlias);
        }
        if (requireDataStream != null) {
            request.setRequireDataStream(requireDataStream);
        }
        if (opType != null) {
            request.opType(opType);
        }
        if (create != null) {
            request.create(create);
        }
        if (version != null) {
            request.version(version);
        }
        if (versionType != null) {
            request.versionType(versionType);
        }
        return request;
    }
}
