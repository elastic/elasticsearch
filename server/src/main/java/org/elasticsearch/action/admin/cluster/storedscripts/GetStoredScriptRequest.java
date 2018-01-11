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
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetStoredScriptRequest extends MasterNodeReadRequest<GetStoredScriptRequest> {

    protected String id;

    GetStoredScriptRequest() {
        super();
    }

    public GetStoredScriptRequest(String id) {
        super();

        this.id = id;
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

    public GetStoredScriptRequest id(String id) {
        this.id = id;

        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        if (in.getVersion().before(Version.V_6_0_0_alpha2)) {
            in.readString(); // read lang from previous versions
        }

        id = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        if (out.getVersion().before(Version.V_6_0_0_alpha2)) {
            out.writeString(""); // write an empty lang to previous versions
        }

        out.writeString(id);
    }

    @Override
    public String toString() {
        return "get script [" + id + "]";
    }
}
