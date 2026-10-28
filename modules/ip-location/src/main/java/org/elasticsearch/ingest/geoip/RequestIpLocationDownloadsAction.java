/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.iplocation.api.IpLocationConsumer;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Transport action for mutating the {@link IpLocationDownloadConsumers} project custom: registers a new
 * {@link IpLocationConsumer} for a project, or unregisters an existing one. Both directions are handled via
 * {@link Request#operation()}; see {@link Request.Operation} for the available values. Routed to the master node so
 * that all consumer updates are serialized through cluster state updates.
 */
public class RequestIpLocationDownloadsAction extends ActionType<AcknowledgedResponse> {

    public static final RequestIpLocationDownloadsAction INSTANCE = new RequestIpLocationDownloadsAction();
    public static final String NAME = "cluster:admin/ip_location/downloads/consumers/update";

    private RequestIpLocationDownloadsAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeRequest<Request> {

        /**
         * Which direction of mutation this request represents against {@link IpLocationDownloadConsumers}.
         */
        public enum Operation {
            REGISTER,
            UNREGISTER;

            /** Lower-case label suitable for embedding in log messages and cluster state update task sources. */
            public String label() {
                return name().toLowerCase(Locale.ROOT);
            }
        }

        private final ProjectId projectId;
        private final IpLocationConsumer consumer;
        private final Operation operation;

        public Request(TimeValue masterNodeTimeout, ProjectId projectId, IpLocationConsumer consumer, Operation operation) {
            super(masterNodeTimeout);
            this.projectId = Objects.requireNonNull(projectId);
            this.consumer = Objects.requireNonNull(consumer);
            this.operation = Objects.requireNonNull(operation);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.projectId = ProjectId.readFrom(in);
            this.consumer = in.readEnum(IpLocationConsumer.class);
            this.operation = in.readEnum(Operation.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            projectId.writeTo(out);
            out.writeEnum(consumer);
            out.writeEnum(operation);
        }

        public ProjectId getProjectId() {
            return projectId;
        }

        public IpLocationConsumer getConsumer() {
            return consumer;
        }

        public Operation operation() {
            return operation;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request that = (Request) o;
            return operation == that.operation
                && Objects.equals(projectId, that.projectId)
                && consumer == that.consumer
                && Objects.equals(masterNodeTimeout(), that.masterNodeTimeout());
        }

        @Override
        public int hashCode() {
            return Objects.hash(masterNodeTimeout(), projectId, consumer, operation);
        }

        @Override
        public String toString() {
            return "RequestIpLocationDownloadsAction.Request{projectId="
                + projectId
                + ", consumer="
                + consumer
                + ", operation="
                + operation
                + '}';
        }
    }
}
