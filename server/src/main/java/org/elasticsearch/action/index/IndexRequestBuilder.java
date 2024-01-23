/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

/**
 * An index document action request builder.
 */
public class IndexRequestBuilder extends ReplicationRequestBuilder<IndexRequest, DocWriteResponse, IndexRequestBuilder>
    implements
        WriteRequestBuilder<IndexRequestBuilder> {
    private String index;
    private String id = null;
    private Map<String, ?> sourceMap;
    private Object[] sourceArray;
    private XContentBuilder sourceXContentBuilder;
    private String sourceString;
    private BytesReference sourceBytesReference;
    private byte[] sourceBytes;
    private Integer sourceOffset;
    private Integer sourceLength;
    private XContentType sourceContentType;
    private String pipeline;
    private Boolean requireAlias;
    private Boolean requireDataStream;
    private String routing;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private String refreshPolicyString;
    private Long ifSeqNo;
    private Long ifPrimaryTerm;
    private String timeoutString;
    private TimeValue timeout;
    private DocWriteRequest.OpType opType;
    private Boolean create;
    private Long version;
    private VersionType versionType;
    private ActiveShardCount waitForActiveShards;

    public IndexRequestBuilder(ElasticsearchClient client) {
        super(client, TransportIndexAction.TYPE, null);
    }

    public IndexRequestBuilder(ElasticsearchClient client, @Nullable String index) {
        super(client, TransportIndexAction.TYPE, null);
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
        this.sourceMap = source;
        return this;
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public IndexRequestBuilder setSource(Map<String, ?> source, XContentType contentType) {
        this.sourceMap = source;
        this.sourceContentType = contentType;
        return this;
    }

    /**
     * Sets the document source to index.
     * <p>
     * Note, its preferable to either set it using {@link #setSource(org.elasticsearch.xcontent.XContentBuilder)}
     * or using the {@link #setSource(byte[], XContentType)}.
     */
    public IndexRequestBuilder setSource(String source, XContentType xContentType) {
        this.sourceString = source;
        this.sourceContentType = xContentType;
        return this;
    }

    /**
     * Sets the content source to index.
     */
    public IndexRequestBuilder setSource(XContentBuilder sourceBuilder) {
        this.sourceXContentBuilder = sourceBuilder;
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequestBuilder setSource(byte[] source, XContentType xContentType) {
        this.sourceBytes = source;
        this.sourceContentType = xContentType;
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
        this.sourceBytes = source;
        this.sourceOffset = offset;
        this.sourceLength = length;
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
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("The number of object passed must be even but was [" + source.length + "]");
        }
        this.sourceArray = source;
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
        this.sourceArray = source;
        this.sourceContentType = xContentType;
        return this;
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
        this.refreshPolicyString = refreshPolicy;
        return this;
    }

    /*
     * The following come from ReplicationRequestBuilder and can be moved to a parent class again in the future
     */

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public IndexRequestBuilder setTimeout(TimeValue timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    public final IndexRequestBuilder setTimeout(String timeout) {
        this.timeoutString = timeout;
        return this;
    }

    public final IndexRequestBuilder setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getIndex() {
        return index;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the write.
     * See {@link ReplicationRequest#waitForActiveShards(ActiveShardCount)} for details.
     */
    public IndexRequestBuilder setWaitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return this;
    }

    /**
     * A shortcut for {@link #setWaitForActiveShards(ActiveShardCount)} where the numerical
     * shard count is passed in, instead of having to first call {@link ActiveShardCount#from(int)}
     * to get the ActiveShardCount.
     */
    public IndexRequestBuilder setWaitForActiveShards(final int waitForActiveShards) {
        return setWaitForActiveShards(ActiveShardCount.from(waitForActiveShards));
    }

    @Override
    public IndexRequest request() {
        IndexRequest indexRequest = new IndexRequest(index);
        try {
            indexRequest.id(id);
            if (sourceMap != null) {
                if (sourceContentType == null) {
                    indexRequest.source(sourceMap);
                } else {
                    indexRequest.source(sourceMap, sourceContentType);
                }
            }
            if (sourceArray != null) {
                if (sourceContentType == null) {
                    indexRequest.source(sourceArray);
                } else {
                    indexRequest.source(sourceContentType, sourceArray);
                }
            }
            if (sourceXContentBuilder != null) {
                indexRequest.source(sourceXContentBuilder);
            }
            if (sourceString != null && sourceContentType != null) {
                indexRequest.source(sourceString, sourceContentType);
            }
            if (sourceBytesReference != null && sourceContentType != null) {
                indexRequest.source(sourceBytesReference, sourceContentType);
            }
            if (sourceBytes != null && sourceContentType != null) {
                if (sourceOffset != null && sourceLength != null) {
                    indexRequest.source(sourceBytes, sourceOffset, sourceLength, sourceContentType);
                } else {
                    indexRequest.source(sourceBytes, sourceContentType);
                }
            }
            if (pipeline != null) {
                indexRequest.setPipeline(pipeline);
            }
            if (routing != null) {
                indexRequest.routing(routing);
            }
            if (refreshPolicy != null) {
                indexRequest.setRefreshPolicy(refreshPolicy);
            }
            if (refreshPolicyString != null) {
                indexRequest.setRefreshPolicy(refreshPolicyString);
            }
            if (timeoutString != null) {
                indexRequest.timeout(timeoutString);
            }
            if (timeout != null) {
                indexRequest.timeout(timeout);
            }
            if (ifSeqNo != null) {
                indexRequest.setIfSeqNo(ifSeqNo);
            }
            if (ifPrimaryTerm != null) {
                indexRequest.setIfPrimaryTerm(ifPrimaryTerm);
            }
            if (pipeline != null) {
                indexRequest.setPipeline(pipeline);
            }
            if (requireAlias != null) {
                indexRequest.setRequireAlias(requireAlias);
            }
            if (requireDataStream != null) {
                indexRequest.setRequireDataStream(requireDataStream);
            }
            if (opType != null) {
                indexRequest.opType(opType);
            }
            if (create != null) {
                indexRequest.create(create);
            }
            if (version != null) {
                indexRequest.version(version);
            }
            if (versionType != null) {
                indexRequest.versionType(versionType);
            }
            if (waitForActiveShards != null) {
                indexRequest.waitForActiveShards(waitForActiveShards);
            }
            return indexRequest;
        } catch (Exception e) {
            indexRequest.decRef();
            throw e;
        }
    }
}
