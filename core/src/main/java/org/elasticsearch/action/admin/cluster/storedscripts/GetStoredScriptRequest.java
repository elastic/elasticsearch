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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetStoredScriptRequest extends MasterNodeReadRequest<GetStoredScriptRequest> {

    protected String id;
    protected String lang;

    GetStoredScriptRequest() {
    }

    public GetStoredScriptRequest(String lang, String id) {
        this.lang = lang;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (lang == null) {
            validationException = ValidateActions.addValidationError("lang is missing", validationException);
        }
        if (id == null) {
            validationException = ValidateActions.addValidationError("id is missing", validationException);
        }
        return validationException;
    }

    public GetStoredScriptRequest lang(@Nullable String type) {
        this.lang = type;
        return this;
    }

    public GetStoredScriptRequest id(String id) {
        this.id = id;
        return this;
    }


    public String lang() {
        return lang;
    }

    public String id() {
        return id;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        lang = in.readString();
        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(lang);
        out.writeString(id);
    }

    @Override
    public String toString() {
        return "get script [" + lang + "][" + id + "]";
    }
}
