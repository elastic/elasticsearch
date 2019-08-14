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
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class GetStoredScriptRequest extends MasterNodeReadRequest<GetStoredScriptRequest> {

    private String[] names;

    public GetStoredScriptRequest() {
        this(new String[]{});
    }

    public GetStoredScriptRequest(String... names) {
        this.names = names;
    }

    public GetStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        names = in.readStringArray();
    }

    /**
     * Returns he names of the scripts.
     */
    public String[] names() {
        return this.names;
    }

    /**
     * @deprecated - Needed for backwards compatibility.
     * Use {@link #names()} instead
     *
     * Return the script if there is only one defined, null otherwise
     */
    @Deprecated
    public String id() {
        return names != null && names.length == 1 ? names[0] : null;
    }

    /**
     * @deprecated - Needed for backwards compatibility.
     * Set the script names param instead
     */
    @Deprecated
    public GetStoredScriptRequest id(String id) {
        this.names = new String[] { id };

        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (names == null) {
            validationException = addValidationError("names is null or empty", validationException);
        } else {
            for (String name : names) {
                if (name == null || !Strings.hasText(name)) {
                    validationException = addValidationError("name is missing", validationException);
                }
            }
        }
        return validationException;
    }

    @Override
    public String toString() {
        return "get script";
    }
}
