/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetStatusAction extends ActionType<GetStatusAction.Response> {
    public static final GetStatusAction INSTANCE = new GetStatusAction();
    public static final String NAME = "cluster:monitor/profiling/status/get";

    protected GetStatusAction() {
        super(NAME);
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final boolean profilingEnabled;
        private final boolean resourceManagementEnabled;
        // true only when ECS k/v indices and templates are fully installed; always false in OTel-only mode
        private final boolean ecsResourcesCreated;
        private final boolean pre891Data;
        private final boolean hasData;
        private boolean timedOut;

        public Response(StreamInput in) throws IOException {
            profilingEnabled = in.readBoolean();
            resourceManagementEnabled = in.readBoolean();
            ecsResourcesCreated = in.readBoolean();
            pre891Data = in.readBoolean();
            timedOut = in.readBoolean();
            hasData = in.readBoolean();
        }

        public Response(
            boolean profilingEnabled,
            boolean resourceManagementEnabled,
            boolean ecsResourcesCreated,
            boolean pre891Data,
            boolean hasData
        ) {
            this.profilingEnabled = profilingEnabled;
            this.resourceManagementEnabled = resourceManagementEnabled;
            this.ecsResourcesCreated = ecsResourcesCreated;
            this.pre891Data = pre891Data;
            this.hasData = hasData;
        }

        public void setTimedOut(boolean timedOut) {
            this.timedOut = timedOut;
        }

        public boolean isEcsResourcesCreated() {
            return ecsResourcesCreated;
        }

        public boolean hasData() {
            return hasData;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startObject("profiling").field("enabled", profilingEnabled).endObject();
            builder.startObject("resource_management").field("enabled", resourceManagementEnabled).endObject();
            builder.startObject("resources")
                .field("created", ecsResourcesCreated)
                .field("pre_8_9_1_data", pre891Data)
                .field("has_data", hasData)
                .endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(profilingEnabled);
            out.writeBoolean(resourceManagementEnabled);
            out.writeBoolean(ecsResourcesCreated);
            out.writeBoolean(pre891Data);
            out.writeBoolean(timedOut);
            out.writeBoolean(hasData);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return profilingEnabled == response.profilingEnabled
                && resourceManagementEnabled == response.resourceManagementEnabled
                && ecsResourcesCreated == response.ecsResourcesCreated
                && pre891Data == response.pre891Data
                && hasData == response.hasData
                && timedOut == response.timedOut;
        }

        @Override
        public int hashCode() {
            return Objects.hash(profilingEnabled, resourceManagementEnabled, ecsResourcesCreated, pre891Data, hasData, timedOut);
        }

        @Override
        public String toString() {
            return Strings.toString(this, true, true);
        }

        public RestStatus status() {
            return timedOut ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
        }
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final boolean waitForEcsResourcesCreated;
        private final TimeValue waitForEcsResourcesCreatedTimeout;

        public Request(StreamInput in) throws IOException {
            super(in);
            waitForEcsResourcesCreatedTimeout = in.readTimeValue();
            waitForEcsResourcesCreated = in.readBoolean();
        }

        public Request(TimeValue masterNodeTimeout, boolean waitForEcsResourcesCreated, TimeValue waitForEcsResourcesCreatedTimeout) {
            super(masterNodeTimeout);
            this.waitForEcsResourcesCreated = waitForEcsResourcesCreated;
            this.waitForEcsResourcesCreatedTimeout = waitForEcsResourcesCreatedTimeout;
        }

        public boolean waitForEcsResourcesCreated() {
            return waitForEcsResourcesCreated;
        }

        public TimeValue waitForEcsResourcesCreatedTimeout() {
            return waitForEcsResourcesCreatedTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeTimeValue(waitForEcsResourcesCreatedTimeout);
            out.writeBoolean(waitForEcsResourcesCreated);
        }
    }
}
