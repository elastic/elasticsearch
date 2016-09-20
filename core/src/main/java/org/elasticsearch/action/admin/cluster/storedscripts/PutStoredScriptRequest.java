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
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.ScriptMetaData.StoredScriptSource;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> {

    private String id;
    private StoredScriptSource source;

    public PutStoredScriptRequest() {
        super();
    }


    public PutStoredScriptRequest(String id, StoredScriptSource source) {
        super();

        this.id = id;
        this.source = source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (id == null) {
            validationException = addValidationError("id is missing", validationException);
        } else if (id.contains("/")) {
            validationException = addValidationError("illegal id [" + id + "] contains [/]", validationException);
        }

        if (source == null) {
            validationException = addValidationError("template/script is missing", validationException);
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

    public StoredScriptSource source() {
        return source;
    }

    public PutStoredScriptRequest source(StoredScriptSource source) {
        this.source = source;

        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        id = in.readString();
        source = StoredScriptSource.staticReadFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeString(id);
        source.writeTo(out);
    }

    @Override
    public String toString() {
        return "PutStoredScriptRequest{" +
            "id='" + id + '\'' +
            ", source=" + source +
            '}';
    }
}
