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
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

@UpdateForV9 // make this class a regular ActionRequest rather than a MasterNodeReadRequest
public class GetAliasesRequest extends MasterNodeReadRequest<GetAliasesRequest> implements AliasesRequest {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandHidden();

    private String[] indices = Strings.EMPTY_ARRAY;
    private String[] aliases = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private String[] originalAliases = Strings.EMPTY_ARRAY;

    public GetAliasesRequest(String... aliases) {
        this.aliases = aliases;
        this.originalAliases = aliases;
    }

    public GetAliasesRequest() {}

    /**
     * NB prior to 8.12 get-aliases was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until we no
     * longer need to support calling this action remotely. Once we remove this we can also make this class a regular ActionRequest instead
     * of a MasterNodeReadRequest.
     */
    @UpdateForV9 // remove this constructor
    public GetAliasesRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        aliases = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        originalAliases = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
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

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
