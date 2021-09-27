/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

public class PutPipelineRequest extends AcknowledgedRequest<PutPipelineRequest> implements ToXContentObject {

    private final String id;
    private final BytesReference source;
    private final XContentType xContentType;
    private boolean versionedUpdate;
    private Integer version;

    /**
     * Create a new pipeline request with the id and source along with the content type of the source
     */
    public PutPipelineRequest(String id, BytesReference source, XContentType xContentType, boolean versionedUpdate, Integer version) {
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
        this.versionedUpdate = versionedUpdate;
        this.version = version;
    }

    public PutPipelineRequest(String id, BytesReference source, XContentType xContentType) {
        this(id, source, xContentType, false, null);
    }

    public PutPipelineRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        source = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            versionedUpdate = in.readBoolean();
            version = in.readOptionalInt();
        } else {
            versionedUpdate = false;
            version = -1;
        }
    }

    PutPipelineRequest() {
        this(null, null, null, false, null);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getId() {
        return id;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    public boolean isVersionedUpdate() {
        return versionedUpdate;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public void setVersionedUpdate(boolean versionedUpdate) {
        this.versionedUpdate = versionedUpdate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeBytesReference(source);
        XContentHelper.writeTo(out, xContentType);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeBoolean(versionedUpdate);
            out.writeOptionalInt(version);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (source != null) {
            builder.rawValue(source.streamInput(), xContentType);
        } else {
            builder.startObject().endObject();
        }
        return builder;
    }
}
