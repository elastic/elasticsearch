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

    public ClearPrivilegesCacheRequest names(String... applicationNames) {
        this.applicationNames = applicationNames;
        return this;
    }

    public String[] names() {
        return applicationNames;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStringArray(applicationNames);
    }

    public static class Node extends TransportRequest {
        private String[] names;

        public Node(StreamInput in) throws IOException {
            super(in);
            names = in.readOptionalStringArray();
        }

        public Node(ClearPrivilegesCacheRequest request) {
            this.names = request.names();
        }

        public String[] getNames() {
            return names;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(names);
        }
    }
}
