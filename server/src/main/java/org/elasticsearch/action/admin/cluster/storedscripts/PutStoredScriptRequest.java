/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> implements ToXContentFragment {

    @Nullable
    private final String id;

    @Nullable
    private final String context;

    private final int contentLength;

    private final StoredScriptSource source;

    public PutStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        if (in.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0)
            || in.getTransportVersion().onOrAfter(TransportVersions.STORED_SCRIPT_CONTENT_LENGTH)) {
            contentLength = in.readVInt();
        } else {
            BytesReference content = in.readBytesReference();
            contentLength = content.length();

            in.readEnum(XContentType.class);    // and drop
        }
        context = in.readOptionalString();
        source = new StoredScriptSource(in);
    }

    public PutStoredScriptRequest(
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        @Nullable String id,
        @Nullable String context,
        int contentLength,
        StoredScriptSource source
    ) {
        super(masterNodeTimeout, ackTimeout);
        this.id = id;
        this.context = context;
        this.contentLength = contentLength;
        this.source = Objects.requireNonNull(source);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored script", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id cannot contain '#' for stored script", validationException);
        }

        return validationException;
    }

    public String id() {
        return id;
    }

    public String context() {
        return context;
    }

    public int contentLength() {
        return contentLength;
    }

    public StoredScriptSource source() {
        return source;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        if (out.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0)
            || out.getTransportVersion().onOrAfter(TransportVersions.STORED_SCRIPT_CONTENT_LENGTH)) {
            out.writeVInt(contentLength);
        } else {
            // generate a bytes reference of the correct size (the content isn't actually used in 8.18)
            out.writeBytesReference(new BytesArray(new byte[contentLength]));
            XContentHelper.writeTo(out, XContentType.JSON); // value not actually used by 8.18
        }
        out.writeOptionalString(context);
        source.writeTo(out);
    }

    @Override
    public String toString() {
        return Strings.format(
            "put stored script {id [%s]%s, content [%s]}",
            id,
            context != null ? ", context [" + context + "]" : "",
            source
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field("script", source, params);
    }
}
