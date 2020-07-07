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

    private String[] applicationNames;
    private boolean clearRolesCache = false;

    public ClearPrivilegesCacheRequest() {
        super((String[]) null);
    }

    public ClearPrivilegesCacheRequest(StreamInput in) throws IOException {
        super(in);
        applicationNames = in.readOptionalStringArray();
        clearRolesCache = in.readBoolean();
    }

    public ClearPrivilegesCacheRequest applicationNames(String... applicationNames) {
        this.applicationNames = applicationNames;
        return this;
    }

    public ClearPrivilegesCacheRequest clearRolesCache(boolean clearRolesCache) {
        this.clearRolesCache = clearRolesCache;
        return this;
    }

    public String[] applicationNames() {
        return applicationNames;
    }

    public boolean clearRolesCache() {
        return clearRolesCache;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStringArray(applicationNames);
        out.writeBoolean(clearRolesCache);
    }

    public static class Node extends TransportRequest {
        private String[] applicationNames;
        private boolean clearRolesCache;

        public Node(StreamInput in) throws IOException {
            super(in);
            applicationNames = in.readOptionalStringArray();
            clearRolesCache = in.readBoolean();
        }

        public Node(ClearPrivilegesCacheRequest request) {
            this.applicationNames = request.applicationNames();
            this.clearRolesCache = request.clearRolesCache;
        }

        public String[] getApplicationNames() {
            return applicationNames;
        }

        public boolean clearRolesCache() {
            return clearRolesCache;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(applicationNames);
            out.writeBoolean(clearRolesCache);
        }
    }
}
