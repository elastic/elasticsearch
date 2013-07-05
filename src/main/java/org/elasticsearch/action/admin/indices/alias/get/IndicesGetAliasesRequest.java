/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IgnoreIndices;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class IndicesGetAliasesRequest extends MasterNodeOperationRequest<IndicesGetAliasesRequest> {

    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] aliases = Strings.EMPTY_ARRAY;

    private IgnoreIndices ignoreIndices = IgnoreIndices.NONE;

    public IndicesGetAliasesRequest(String[] aliases) {
        this.aliases = aliases;
    }

    public IndicesGetAliasesRequest(String alias) {
        this.aliases = new String[]{alias};
    }

    public IndicesGetAliasesRequest() {
    }

    public IndicesGetAliasesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public IndicesGetAliasesRequest aliases(String... aliases) {
        this.aliases = aliases;
        return this;
    }

    public IndicesGetAliasesRequest ignoreIndices(IgnoreIndices ignoreIndices) {
        this.ignoreIndices = ignoreIndices;
        return this;
    }

    public String[] indices() {
        return indices;
    }

    public String[] aliases() {
        return aliases;
    }

    public IgnoreIndices ignoreIndices() {
        return ignoreIndices;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (aliases.length == 0) {
            return addValidationError("No alias specified", null);
        } else {
            return null;
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readStringArray();
        aliases = in.readStringArray();
        ignoreIndices = IgnoreIndices.fromId(in.readByte());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeStringArray(aliases);
        out.writeByte(ignoreIndices.id());
    }
}
