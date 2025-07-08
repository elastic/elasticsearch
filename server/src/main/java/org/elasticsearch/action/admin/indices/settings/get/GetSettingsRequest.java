/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class GetSettingsRequest extends LocalClusterStateRequest implements IndicesRequest.Replaceable {

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, true, true, true);

    private String[] indices = Strings.EMPTY_ARRAY;
    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
    private String[] names = Strings.EMPTY_ARRAY;
    private boolean humanReadable = false;
    private boolean includeDefaults = false;

    @Override
    public GetSettingsRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    public GetSettingsRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * When include_defaults is set, return default values which are normally suppressed.
     * This flag is specific to the rest client.
     */
    public GetSettingsRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    public GetSettingsRequest(TimeValue masterTimeout) {
        super(masterTimeout);
    }

    /**
     * NB prior to 9.1 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
     * we no longer need to support calling this action remotely.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    public GetSettingsRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        names = in.readStringArray();
        humanReadable = in.readBoolean();
        includeDefaults = in.readBoolean();
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public String[] names() {
        return names;
    }

    public GetSettingsRequest names(String... names) {
        this.names = names;
        return this;
    }

    public boolean humanReadable() {
        return humanReadable;
    }

    public GetSettingsRequest humanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
        return this;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (names == null) {
            validationException = ValidateActions.addValidationError("names may not be null", validationException);
        }
        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSettingsRequest that = (GetSettingsRequest) o;
        return humanReadable == that.humanReadable
            && includeDefaults == that.includeDefaults
            && Arrays.equals(indices, that.indices)
            && Objects.equals(indicesOptions, that.indicesOptions)
            && Arrays.equals(names, that.names);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indicesOptions, humanReadable, includeDefaults);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(names);
        return result;
    }
}
