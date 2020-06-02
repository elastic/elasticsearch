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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetStoredScriptRequest extends MasterNodeReadRequest<GetStoredScriptRequest> {

    private String[] ids;

    public GetStoredScriptRequest() {
        this(new String[]{});
    }

    public GetStoredScriptRequest(String... ids) {
        this.ids = ids;
    }

    public GetStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            this.ids = in.readStringArray();
        } else {
            this.ids = new String[] { in.readString() };
        }
    }

    /**
     * Returns the ids of the scripts.
     */
    public String[] ids() {
        return this.ids;
    }

    public GetStoredScriptRequest ids(String... ids) {
        this.ids = ids;

        return this;
    }

    /**
     * @deprecated - Needed for backwards compatibility.
     * Use {@link #ids()} instead
     *
     * Return the only script
     */
    @Deprecated
    public String id() {
        assert(ids.length == 1);
        return ids[0];
    }

    /**
     * @deprecated - Needed for backwards compatibility.
     * Set the script ids param instead
     */
    @Deprecated
    public GetStoredScriptRequest id(String id) {
        this.ids = new String[] { id };

        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeStringArray(ids);
        } else {
            out.writeString(ids[0]);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (ids == null) {
            validationException = addValidationError("ids is null or empty", validationException);
        } else {
            for (String name : ids) {
                if (name == null || !Strings.hasText(name)) {
                    validationException = addValidationError("id is missing", validationException);
                }
            }
        }
        return validationException;
    }

    @Override
    public String toString() {
        return "get script[ " + String.join(", ", ids) + "]";
    }
}
