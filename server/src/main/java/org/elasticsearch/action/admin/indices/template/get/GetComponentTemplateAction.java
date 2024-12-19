/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.support.local.LocalClusterStateRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Action to retrieve one or more component templates
 */
public class GetComponentTemplateAction extends ActionType<GetComponentTemplateAction.Response> {

    public static final GetComponentTemplateAction INSTANCE = new GetComponentTemplateAction();
    public static final String NAME = "cluster:admin/component_template/get";

    private GetComponentTemplateAction() {
        super(NAME);
    }

    /**
     * Request that to retrieve one or more component templates
     */
    public static class Request extends LocalClusterStateRequest {

        @Nullable
        private String name;
        private boolean includeDefaults;

        public Request(TimeValue masterTimeout, String name) {
            super(masterTimeout);
            this.name = name;
            this.includeDefaults = false;
        }

        /**
         * NB prior to 9.0 get-component was a TransportMasterNodeReadAction so for BwC we must remain able to read these requests until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readOptionalString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                includeDefaults = in.readBoolean();
            } else {
                includeDefaults = false;
            }
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }

        /**
         * Sets the name of the component templates.
         */
        public Request name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the flag to signal that in the response the default values will also be displayed.
         */
        public Request includeDefaults(boolean includeDefaults) {
            this.includeDefaults = includeDefaults;
            return this;
        }

        /**
         * The name of the component templates.
         */
        public String name() {
            return this.name;
        }

        /**
         * True if in the response the default values will be displayed.
         */
        public boolean includeDefaults() {
            return includeDefaults;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        public static final ParseField NAME = new ParseField("name");
        public static final ParseField COMPONENT_TEMPLATES = new ParseField("component_templates");
        public static final ParseField COMPONENT_TEMPLATE = new ParseField("component_template");

        private final Map<String, ComponentTemplate> componentTemplates;
        @Nullable
        private final RolloverConfiguration rolloverConfiguration;

        /**
         * Please use {@link GetComponentTemplateAction.Response#Response(Map)}
         */
        @Deprecated
        public Response(Map<String, ComponentTemplate> componentTemplates, @Nullable DataStreamGlobalRetention globalRetention) {
            this(componentTemplates, (RolloverConfiguration) null);
        }

        /**
         * Please use {@link GetComponentTemplateAction.Response#Response(Map, RolloverConfiguration)}
         */
        @Deprecated
        public Response(
            Map<String, ComponentTemplate> componentTemplates,
            @Nullable RolloverConfiguration rolloverConfiguration,
            @Nullable DataStreamGlobalRetention ignored
        ) {
            this(componentTemplates, rolloverConfiguration);
        }

        public Response(Map<String, ComponentTemplate> componentTemplates) {
            this(componentTemplates, (RolloverConfiguration) null);
        }

        public Response(Map<String, ComponentTemplate> componentTemplates, @Nullable RolloverConfiguration rolloverConfiguration) {
            this.componentTemplates = componentTemplates;
            this.rolloverConfiguration = rolloverConfiguration;
        }

        public Map<String, ComponentTemplate> getComponentTemplates() {
            return componentTemplates;
        }

        public RolloverConfiguration getRolloverConfiguration() {
            return rolloverConfiguration;
        }

        /**
         * @return null
         * @deprecated The global retention is not used anymore in the component template response
         */
        @Deprecated
        @Nullable
        public DataStreamGlobalRetention getGlobalRetention() {
            return null;
        }

        /**
         * NB prior to 9.0 get-component was a TransportMasterNodeReadAction so for BwC we must remain able to write these responses until
         * we no longer need to support calling this action remotely.
         */
        @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(componentTemplates, StreamOutput::writeWriteable);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
                out.writeOptionalWriteable(rolloverConfiguration);
            }
            if (out.getTransportVersion().between(TransportVersions.V_8_14_0, TransportVersions.V_8_16_0)) {
                out.writeOptionalWriteable(null);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(componentTemplates, that.componentTemplates)
                && Objects.equals(rolloverConfiguration, that.rolloverConfiguration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(componentTemplates, rolloverConfiguration);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray(COMPONENT_TEMPLATES.getPreferredName());
            for (Map.Entry<String, ComponentTemplate> componentTemplate : this.componentTemplates.entrySet()) {
                builder.startObject();
                builder.field(NAME.getPreferredName(), componentTemplate.getKey());
                builder.field(COMPONENT_TEMPLATE.getPreferredName());
                componentTemplate.getValue().toXContent(builder, params, rolloverConfiguration);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            return builder;
        }

    }
}
