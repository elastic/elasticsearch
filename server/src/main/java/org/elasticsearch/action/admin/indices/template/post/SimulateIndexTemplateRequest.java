/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.post;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class SimulateIndexTemplateRequest extends LocalClusterStateRequest {

    private String indexName;

    @Nullable
    private TransportPutComposableIndexTemplateAction.Request indexTemplateRequest;

    private boolean includeDefaults = false;

    public SimulateIndexTemplateRequest(TimeValue masterTimeout, String indexName) {
        super(masterTimeout);
        if (Strings.isNullOrEmpty(indexName)) {
            throw new IllegalArgumentException("index name cannot be null or empty");
        }
        this.indexName = indexName;
    }

    /**
     * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
     * we no longer need to support calling this action remotely.
     */
    public SimulateIndexTemplateRequest(StreamInput in) throws IOException {
        super(in);
        indexName = in.readString();
        indexTemplateRequest = in.readOptionalWriteable(TransportPutComposableIndexTemplateAction.Request::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            includeDefaults = in.readBoolean();
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexTemplateRequest != null) {
            validationException = indexTemplateRequest.validateIndexTemplate(validationException);
        }
        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }

    public String getIndexName() {
        return indexName;
    }

    public boolean includeDefaults() {
        return includeDefaults;
    }

    @Nullable
    public TransportPutComposableIndexTemplateAction.Request getIndexTemplateRequest() {
        return indexTemplateRequest;
    }

    public SimulateIndexTemplateRequest indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public SimulateIndexTemplateRequest indexTemplateRequest(TransportPutComposableIndexTemplateAction.Request indexTemplateRequest) {
        this.indexTemplateRequest = indexTemplateRequest;
        return this;
    }

    public SimulateIndexTemplateRequest includeDefaults(boolean includeDefaults) {
        this.includeDefaults = includeDefaults;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimulateIndexTemplateRequest that = (SimulateIndexTemplateRequest) o;
        return indexName.equals(that.indexName)
            && Objects.equals(indexTemplateRequest, that.indexTemplateRequest)
            && includeDefaults == that.includeDefaults;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, indexTemplateRequest, includeDefaults);
    }
}
