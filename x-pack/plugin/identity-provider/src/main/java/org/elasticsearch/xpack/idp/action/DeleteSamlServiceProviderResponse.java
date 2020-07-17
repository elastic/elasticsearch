/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Response object after removing a service provider from the IdP.
 */
public class DeleteSamlServiceProviderResponse extends ActionResponse implements ToXContentObject {

    private final String docId;
    private final long seqNo;
    private final long primaryTerm;
    private final String entityId;

    public DeleteSamlServiceProviderResponse(String docId, long seqNo, long primaryTerm, String entityId) {
        this.docId = docId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.entityId = Objects.requireNonNull(entityId, "Entity Id cannot be null");
    }

    public DeleteSamlServiceProviderResponse(DeleteResponse deleteResponse, String entityId) {
        this(deleteResponse == null ? null : deleteResponse.getId(),
            deleteResponse == null ? UNASSIGNED_SEQ_NO : deleteResponse.getSeqNo(),
            deleteResponse == null ? UNASSIGNED_PRIMARY_TERM : deleteResponse.getPrimaryTerm(),
            entityId);
    }

    public DeleteSamlServiceProviderResponse(StreamInput in) throws IOException {
        docId = in.readString();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        entityId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(docId);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeString(entityId);
    }

    @Nullable
    public String getDocId() {
        return docId;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public long getPrimaryTerm() {
        return primaryTerm;
    }

    public String getEntityId() {
        return entityId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("document");
        builder.field("_id", docId);
        builder.field("_seq_no", seqNo);
        builder.field("_primary_term", primaryTerm);
        builder.endObject();

        builder.startObject("service_provider");
        builder.field("entity_id", entityId);
        builder.endObject();

        return builder.endObject();
    }

    public boolean found() {
        return docId != null;
    }
}
