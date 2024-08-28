/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.delete;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteRequestBuilder;
import org.elasticsearch.action.support.replication.ReplicationRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.VersionType;

/**
 * A delete document action request builder.
 */
public class DeleteRequestBuilder extends ReplicationRequestBuilder<DeleteRequest, DeleteResponse, DeleteRequestBuilder>
    implements
        WriteRequestBuilder<DeleteRequestBuilder> {

    private String id;
    private String routing;
    private Long version;
    private VersionType versionType;
    private Long seqNo;
    private Long term;
    private WriteRequest.RefreshPolicy refreshPolicy;

    @SuppressWarnings("this-escape")
    public DeleteRequestBuilder(ElasticsearchClient client, @Nullable String index) {
        super(client, TransportDeleteAction.TYPE);
        setIndex(index);
    }

    /**
     * Sets the id of the document to delete.
     */
    public DeleteRequestBuilder setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the delete request. Using this value to hash the shard
     * and not the id.
     */
    public DeleteRequestBuilder setRouting(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public DeleteRequestBuilder setVersion(long version) {
        this.version = version;
        return this;
    }

    /**
     * Sets the type of versioning to use. Defaults to {@link VersionType#INTERNAL}.
     */
    public DeleteRequestBuilder setVersionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    /**
     * only perform this delete request if the document was last modification was assigned the given
     * sequence number. Must be used in combination with {@link #setIfPrimaryTerm(long)}
     *
     * If the document last modification was assigned a different sequence number a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public DeleteRequestBuilder setIfSeqNo(long seqNo) {
        this.seqNo = seqNo;
        return this;
    }

    /**
     * only perform this delete request if the document was last modification was assigned the given
     * primary term. Must be used in combination with {@link #setIfSeqNo(long)}
     *
     * If the document last modification was assigned a different term a
     * {@link org.elasticsearch.index.engine.VersionConflictEngineException} will be thrown.
     */
    public DeleteRequestBuilder setIfPrimaryTerm(long term) {
        this.term = term;
        return this;
    }

    @Override
    public DeleteRequestBuilder setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public DeleteRequestBuilder setRefreshPolicy(String refreshPolicy) {
        this.refreshPolicy = WriteRequest.RefreshPolicy.parse(refreshPolicy);
        return this;
    }

    @Override
    public DeleteRequest request() {
        DeleteRequest request = new DeleteRequest();
        super.apply(request);
        if (id != null) {
            request.id(id);
        }
        if (routing != null) {
            request.routing(routing);
        }
        if (version != null) {
            request.version(version);
        }
        if (versionType != null) {
            request.versionType(versionType);
        }
        if (seqNo != null) {
            request.setIfSeqNo(seqNo);
        }
        if (term != null) {
            request.setIfPrimaryTerm(term);
        }
        if (refreshPolicy != null) {
            request.setRefreshPolicy(refreshPolicy);
        }
        return request;
    }
}
