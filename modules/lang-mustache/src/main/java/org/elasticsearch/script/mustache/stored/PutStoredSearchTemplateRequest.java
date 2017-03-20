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

package org.elasticsearch.script.mustache.stored;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutStoredSearchTemplateRequest extends AcknowledgedRequest<
        PutStoredSearchTemplateRequest> {
    private String id;
    private BytesReference content;
    private XContentType xContentType;

    public PutStoredSearchTemplateRequest() {
    }

    public PutStoredSearchTemplateRequest(String id, BytesReference content,
            XContentType xContentType) {
        this.id = id;
        this.content = content;
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    public PutStoredSearchTemplateRequest id(String id) {
        this.id = id;
        return this;
    }

    public PutStoredSearchTemplateRequest content(BytesReference content,
            XContentType xContentType) {
        this.content = content;
        this.xContentType = xContentType;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored search template",
                    validationException);
        } else
            if (id.contains("#")) {
                validationException = addValidationError(
                        "id cannot contain '#' for stored search template", validationException);
            }

        if (content == null) {
            validationException = addValidationError("must specify code for stored search template",
                    validationException);
        }

        return validationException;
    }

    public String id() {
        return id;
    }

    public BytesReference content() {
        return content;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readOptionalString();
        content = in.readBytesReference();
        xContentType = XContentType.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBytesReference(content);
        xContentType.writeTo(out);
    }

    @Override
    public String toString() {
        String source = "_na_";

        try {
            source = XContentHelper.convertToJson(content, false, xContentType);
        } catch (Exception e) {
            // ignore
        }

        return "put search template {id [" + id + "], content [" + source + "]}";
    }
}
