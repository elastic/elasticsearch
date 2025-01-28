/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Request for retrieving service account credentials that are local to each node.
 * Currently, this means file-backed service tokens.
 */
public class GetServiceAccountCredentialsNodesRequest extends BaseNodesRequest {

    private final String namespace;
    private final String serviceName;

    public GetServiceAccountCredentialsNodesRequest(String namespace, String serviceName) {
        super((String[]) null);
        this.namespace = namespace;
        this.serviceName = serviceName;
    }

    public static class Node extends TransportRequest {

        private final String namespace;
        private final String serviceName;

        public Node(GetServiceAccountCredentialsNodesRequest request) {
            this.namespace = request.namespace;
            this.serviceName = request.serviceName;
        }

        public Node(StreamInput in) throws IOException {
            super(in);
            this.namespace = in.readString();
            this.serviceName = in.readString();
        }

        public String getNamespace() {
            return namespace;
        }

        public String getServiceName() {
            return serviceName;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(namespace);
            out.writeString(serviceName);
        }
    }
}
