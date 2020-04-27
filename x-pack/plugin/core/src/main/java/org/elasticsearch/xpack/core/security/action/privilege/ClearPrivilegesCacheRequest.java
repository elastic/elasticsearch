/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ClearPrivilegesCacheRequest extends BaseNodesRequest<ClearPrivilegesCacheRequest> {

    String[] applicationNames;

    public ClearPrivilegesCacheRequest() {
        super((String[]) null);
    }

    public ClearPrivilegesCacheRequest(StreamInput in) throws IOException {
        super(in);
        applicationNames = in.readOptionalStringArray();
    }

    public ClearPrivilegesCacheRequest applicationNames(String... applicationNames) {
        this.applicationNames = applicationNames;
        return this;
    }

    public String[] applicationNames() {
        return applicationNames;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStringArray(applicationNames);
    }

    public static class Node extends TransportRequest {
        private String[] applicationNames;

        public Node(StreamInput in) throws IOException {
            super(in);
            applicationNames = in.readOptionalStringArray();
        }

        public Node(ClearPrivilegesCacheRequest request) {
            this.applicationNames = request.applicationNames();
        }

        public String[] getApplicationNames() {
            return applicationNames;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(applicationNames);
        }
    }
}
