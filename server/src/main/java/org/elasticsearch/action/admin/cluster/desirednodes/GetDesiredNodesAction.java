/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static java.util.Collections.singletonMap;

public class GetDesiredNodesAction extends ActionType<GetDesiredNodesAction.Response> {
    public static final GetDesiredNodesAction INSTANCE = new GetDesiredNodesAction();
    public static final String NAME = "cluster:admin/desired_nodes/get";

    GetDesiredNodesAction() {
        super(NAME);
    }

    public static class Request extends MasterNodeReadRequest<Request> {
        public Request(TimeValue masterNodeTimeout) {
            super(masterNodeTimeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {
        private final DesiredNodes desiredNodes;

        public Response(DesiredNodes desiredNodes) {
            this.desiredNodes = desiredNodes;
        }

        public Response(StreamInput in) throws IOException {
            this.desiredNodes = DesiredNodes.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            desiredNodes.writeTo(out);
        }

        public DesiredNodes getDesiredNodes() {
            return desiredNodes;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            return desiredNodes.toXContent(
                builder,
                new DelegatingMapParams(singletonMap(DesiredNodes.CONTEXT_MODE_PARAM, DesiredNodes.CONTEXT_MODE_API), params)
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(desiredNodes, response.desiredNodes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(desiredNodes);
        }
    }
}
