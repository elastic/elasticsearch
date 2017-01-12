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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> {

    private String id;
    private String scriptLang;
    private BytesReference script;
    private XContentType xContentType;

    public PutStoredScriptRequest() {
        super();
    }

    public PutStoredScriptRequest(String scriptLang) {
        super();
        this.scriptLang = scriptLang;
    }

    public PutStoredScriptRequest(String scriptLang, String id) {
        super();
        this.scriptLang = scriptLang;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id can't contain: '#'", validationException);
        }
        if (scriptLang == null) {
            validationException = addValidationError("lang is missing", validationException);
        } else if (scriptLang.contains("#")) {
            validationException = addValidationError("lang can't contain: '#'", validationException);
        }
        if (script == null) {
            validationException = addValidationError("script is missing", validationException);
        }
        return validationException;
    }

    public String scriptLang() {
        return scriptLang;
    }

    public PutStoredScriptRequest scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return this;
    }

    public String id() {
        return id;
    }

    public PutStoredScriptRequest id(String id) {
        this.id = id;
        return this;
    }

    public BytesReference script() {
        return script;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    @Deprecated
    public PutStoredScriptRequest script(BytesReference source) {
        this.script = source;
        return this;
    }

    public PutStoredScriptRequest script(BytesReference source, XContentType xContentType) {
        this.script = source;
        this.xContentType = xContentType;
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        scriptLang = in.readString();
        id = in.readOptionalString();
        script = in.readBytesReference();
        if (in.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED) && in.readBoolean()) {
            xContentType = XContentType.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(scriptLang);
        out.writeOptionalString(id);
        out.writeBytesReference(script);
        if (out.getVersion().onOrAfter(Version.V_5_3_0_UNRELEASED)) {
            if (xContentType == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                xContentType.writeTo(out);
            }
        }
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            if (xContentType == null) {
                sSource = XContentHelper.convertToJson(script, false);
            } else {
                sSource = XContentHelper.convertToJson(script, false, xContentType);
            }
        } catch (Exception e) {
            // ignore
        }
        return "put script {[" + id + "][" + scriptLang + "], script[" + sSource + "]}";
    }
}
