/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.AliasesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

public class GetAliasesRequest extends LocalClusterStateRequest implements AliasesRequest {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandHiddenNoSelectors();

    private String[] aliases;
    private String[] originalAliases;
    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public GetAliasesRequest(TimeValue masterTimeout, String... aliases) {
        super(masterTimeout);
        this.aliases = aliases;
        this.originalAliases = aliases;
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
