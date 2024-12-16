/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Strings;
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

    private final BytesReference content;
    private final XContentType xContentType;
    private final StoredScriptSource source;

    public PutStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        content = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
        context = in.readOptionalString();
        source = new StoredScriptSource(in);
    }

    public PutStoredScriptRequest(
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        @Nullable String id,
        @Nullable String context,
        BytesReference content,
        XContentType xContentType,
        StoredScriptSource source
    ) {
        super(masterNodeTimeout, ackTimeout);
        this.id = id;
        this.context = context;
        this.content = Objects.requireNonNull(content);
        this.xContentType = Objects.requireNonNull(xContentType);
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

    public BytesReference content() {
        return content;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    public StoredScriptSource source() {
        return source;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBytesReference(content);
        XContentHelper.writeTo(out, xContentType);
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
