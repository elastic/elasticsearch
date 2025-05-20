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
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * An action for simulating the complete composed settings of the specified
 * index template name, or index template configuration
 */
public class SimulateTemplateAction extends ActionType<SimulateIndexTemplateResponse> {

    public static final SimulateTemplateAction INSTANCE = new SimulateTemplateAction();
    public static final String NAME = "indices:admin/index_template/simulate";

    private SimulateTemplateAction() {
        super(NAME);
    }

    public static class Request extends LocalClusterStateRequest {

        @Nullable
        private String templateName;

        @Nullable
        private TransportPutComposableIndexTemplateAction.Request indexTemplateRequest;
        private boolean includeDefaults = false;

        public Request(TimeValue masterTimeout, String templateName) {
            super(masterTimeout);
            this.templateName = templateName;
        }

        /**
         * NB prior to 9.0 this was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        public Request(StreamInput in) throws IOException {
            super(in);
            templateName = in.readOptionalString();
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
            if (templateName == null && indexTemplateRequest == null) {
                validationException = ValidateActions.addValidationError(
                    "either index name or index template body must be specified for simulation",
                    validationException
                );
            }
            return validationException;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        @Nullable
        public String getTemplateName() {
            return templateName;
        }

        public boolean includeDefaults() {
            return includeDefaults;
        }

        @Nullable
        public TransportPutComposableIndexTemplateAction.Request getIndexTemplateRequest() {
            return indexTemplateRequest;
        }

        public Request indexTemplateRequest(TransportPutComposableIndexTemplateAction.Request indexTemplateRequest) {
            this.indexTemplateRequest = indexTemplateRequest;
            return this;
        }

        public Request includeDefaults(boolean includeDefaults) {
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
            Request that = (Request) o;
            return templateName.equals(that.templateName)
                && Objects.equals(indexTemplateRequest, that.indexTemplateRequest)
                && includeDefaults == that.includeDefaults;
        }

        @Override
        public int hashCode() {
            return Objects.hash(templateName, indexTemplateRequest, includeDefaults);
        }
    }
}
