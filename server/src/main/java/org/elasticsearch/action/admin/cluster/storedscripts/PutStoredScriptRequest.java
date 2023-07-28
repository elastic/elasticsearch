/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> implements ToXContentFragment {

    private String id;
    private String context;
    private BytesReference content;
    private XContentType xContentType;
    private StoredScriptSource source;

    public PutStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        content = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
        context = in.readOptionalString();
        source = new StoredScriptSource(in);
    }

    public PutStoredScriptRequest() {
        super();
    }

    public PutStoredScriptRequest(String id, String context, BytesReference content, XContentType xContentType, StoredScriptSource source) {
        super();
        this.id = id;
        this.context = context;
        this.content = content;
        this.xContentType = Objects.requireNonNull(xContentType);
        this.source = source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored script", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id cannot contain '#' for stored script", validationException);
        }

        if (content == null) {
            validationException = addValidationError("must specify code for stored script", validationException);
        }

        return validationException;
    }

    public String id() {
        return id;
    }

    public PutStoredScriptRequest id(String id) {
        this.id = id;
        return this;
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

    /**
     * Set the script source and the content type of the bytes.
     */
    public PutStoredScriptRequest content(BytesReference content, XContentType xContentType) {
        this.content = content;
        this.xContentType = Objects.requireNonNull(xContentType);
        this.source = StoredScriptSource.parse(content, xContentType);
        return this;
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
        String source = "_na_";

        try {
            source = XContentHelper.convertToJson(content, false, xContentType);
        } catch (Exception e) {
            // ignore
        }

        return "put stored script {id ["
            + id
            + "]"
            + (context != null ? ", context [" + context + "]" : "")
            + ", content ["
            + source
            + "]}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("script");
        source.toXContent(builder, params);

        return builder;
    }
}
