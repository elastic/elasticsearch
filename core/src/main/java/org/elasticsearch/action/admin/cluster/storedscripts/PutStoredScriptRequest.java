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

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> {

    private String id;
    private String lang;
    private BytesReference content;
    private XContentType xContentType;

    public PutStoredScriptRequest() {
        super();
    }

    public PutStoredScriptRequest(String id, String lang, BytesReference content, XContentType xContentType) {
        super();
        this.id = id;
        this.lang = lang;
        this.content = content;
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored script", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id cannot contain '#' for stored script", validationException);
        }

        if (lang != null && lang.contains("#")) {
            validationException = addValidationError("lang cannot contain '#' for stored script", validationException);
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

    public String lang() {
        return lang;
    }

    public PutStoredScriptRequest lang(String lang) {
        this.lang = lang;

        return this;
    }

    public BytesReference content() {
        return content;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    /**
     * Set the script source and the content type of the bytes.
     */
    public PutStoredScriptRequest content(BytesReference content, XContentType xContentType) {
        this.content = content;
        this.xContentType = Objects.requireNonNull(xContentType);
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        lang = in.readString();

        if (lang.isEmpty()) {
            lang = null;
        }

        id = in.readOptionalString();
        content = in.readBytesReference();
        if (in.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED)) {
            xContentType = XContentType.readFrom(in);
        } else {
            xContentType = XContentFactory.xContentType(content);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeString(lang == null ? "" : lang);
        out.writeOptionalString(id);
        out.writeBytesReference(content);
        if (out.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED)) {
            xContentType.writeTo(out);
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

        return "put stored script {id [" + id + "]" + (lang != null ? ", lang [" + lang + "]" : "") + ", content [" + source + "]}";
    }
}
