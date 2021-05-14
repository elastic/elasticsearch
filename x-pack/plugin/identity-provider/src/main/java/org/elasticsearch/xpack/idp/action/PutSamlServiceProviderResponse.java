/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PutSamlServiceProviderResponse extends ActionResponse implements ToXContentObject {

    private final String docId;
    private final boolean created;
    private final long seqNo;
    private final long primaryTerm;
    private final String entityId;
    private final boolean enabled;

    public PutSamlServiceProviderResponse(String docId, boolean created, long seqNo, long primaryTerm, String entityId, boolean enabled) {
        this.docId = Objects.requireNonNull(docId, "Document Id cannot be null");
        this.created = created;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.entityId = Objects.requireNonNull(entityId, "Entity Id cannot be null");
        this.enabled = enabled;
    }

    public PutSamlServiceProviderResponse(StreamInput in) throws IOException {
        docId = in.readString();
        created = in.readBoolean();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        entityId = in.readString();
        enabled = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(docId);
        out.writeBoolean(created);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeString(entityId);
        out.writeBoolean(enabled);
    }

    public String getDocId() {
        return docId;
    }

    public boolean isCreated() {
        return created;
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

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        builder.startObject("document");
        builder.field("_id", docId);
        builder.field("_created", created);
        builder.field("_seq_no", seqNo);
        builder.field("_primary_term", primaryTerm);
        builder.endObject();

        builder.startObject("service_provider");
        builder.field("entity_id", entityId);
        builder.field("enabled", enabled);
        builder.endObject();

        return builder.endObject();
    }
}
