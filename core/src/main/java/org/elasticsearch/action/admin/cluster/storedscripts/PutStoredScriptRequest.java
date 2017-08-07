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

package org.elasticsearch.action.admin.cluster.storedscripts;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> {

    private String id;
    private String context;
    private BytesReference content;
    private XContentType xContentType;
    private StoredScriptSource source;

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

    public PutStoredScriptRequest context(String context) {
        this.context = context;
        return this;
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.getVersion().before(Version.V_6_0_0_alpha2)) {
            in.readString(); // read lang from previous versions
        }
        id = in.readOptionalString();
        content = in.readBytesReference();
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            xContentType = XContentType.readFrom(in);
        } else {
            xContentType = XContentFactory.xContentType(content);
        }
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha2)) {
            context = in.readOptionalString();
            source = new StoredScriptSource(in);
        } else {
            source = StoredScriptSource.parse(content, xContentType == null ? XContentType.JSON : xContentType);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (out.getVersion().before(Version.V_6_0_0_alpha2)) {
            out.writeString(source == null ? "" : source.getLang());
        }
        out.writeOptionalString(id);
        out.writeBytesReference(content);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            xContentType.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha2)) {
            out.writeOptionalString(context);
            source.writeTo(out);
        }
    }

    @Override
    public String toString() {
        String source = "_na_";

        try {
            source = XContentHelper.convertToJson(content, false, xContentType);
        } catch (Exception e) {
            // ignore
        }

        return "put stored script {id [" + id + "]" +
            (context != null ? ", context [" + context + "]" : "") +
            ", content [" + source + "]}";
    }
}
