/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.warmer.delete;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to delete an index warmer.
 */
public class DeleteWarmerRequest extends AcknowledgedRequest<DeleteWarmerRequest> {

    private String name;

    private String[] indices = Strings.EMPTY_ARRAY;

    DeleteWarmerRequest() {
    }

    /**
     * Constructs a new delete warmer request for the specified name.
     *
     * @param name: the name (or wildcard expression) of the warmer to match, null to delete all.
     */
    public DeleteWarmerRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    /**
     * The name to delete.
     */
    @Nullable
    String name() {
        return name;
    }

    /**
     * The name (or wildcard expression) of the index warmer to delete, or null
     * to delete all warmers.
     */
    public DeleteWarmerRequest name(@Nullable String name) {
        this.name = name;
        return this;
    }

    /**
     * Sets the indices this put mapping operation will execute on.
     */
    public DeleteWarmerRequest indices(String[] indices) {
        this.indices = indices;
        return this;
    }

    /**
     * The indices the mappings will be put.
     */
    public String[] indices() {
        return indices;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readOptionalString();
        indices = in.readStringArray();
        readTimeout(in, Version.V_0_90_6);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
        out.writeStringArrayNullable(indices);
        writeTimeout(out, Version.V_0_90_6);
    }
}
