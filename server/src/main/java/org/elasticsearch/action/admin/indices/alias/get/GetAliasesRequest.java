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
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class GetAliasesRequest extends MasterNodeReadRequest<GetAliasesRequest> implements AliasesRequest {

    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] aliases = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpand();
    private boolean aliasesProvided = false;

    public GetAliasesRequest(String... aliases) {
        this.aliases = Objects.requireNonNull(aliases);
        this.aliasesProvided = aliases.length != 0;
    }

    public GetAliasesRequest() {
    }

    public GetAliasesRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        aliases = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            aliasesProvided = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeStringArray(aliases);
        indicesOptions.writeIndicesOptions(out);
        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeBoolean(aliasesProvided);
        }
    }

    @Override
    public GetAliasesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public GetAliasesRequest aliases(String... aliases) {
        this.aliases = Objects.requireNonNull(aliases);
        this.aliasesProvided = aliases.length != 0;
        return this;
    }

    public GetAliasesRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public String[] aliases() {
        return aliases;
    }

    @Override
    public void replaceAliases(String... aliases) {
        this.aliases = aliases;
    }

    /**
     * @return Whether aliases that where originally provided by the user via {@link #aliases(String...)} or
     *         {@link #GetAliasesRequest(String...)}. If this is not the case and there are aliases then
     */
    boolean isAliasesProvided() {
        return aliasesProvided;
    }

    @Override
    public boolean expandAliasesWildcards() {
        return true;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }
}
