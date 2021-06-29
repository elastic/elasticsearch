/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class GetAliasesRequest extends MasterNodeReadRequest<GetAliasesRequest> implements AliasesRequest {

    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] aliases = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = IndicesOptions.strictExpandHidden();
    private String[] originalAliases = Strings.EMPTY_ARRAY;

    public GetAliasesRequest(String... aliases) {
        this.aliases = aliases;
        this.originalAliases = aliases;
    }

    public GetAliasesRequest() {
    }

    public GetAliasesRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        aliases = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        originalAliases = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        out.writeStringArray(aliases);
        indicesOptions.writeIndicesOptions(out);
        out.writeStringArray(originalAliases);
    }

    @Override
    public GetAliasesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public GetAliasesRequest aliases(String... aliases) {
        this.aliases = aliases;
        this.originalAliases = aliases;
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
     * Returns the aliases as was originally specified by the user
     */
    public String[] getOriginalAliases() {
        return originalAliases;
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
    public boolean includeDataStreams() {
        return true;
    }
}
