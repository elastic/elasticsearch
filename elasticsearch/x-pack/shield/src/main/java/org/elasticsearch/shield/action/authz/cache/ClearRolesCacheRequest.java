/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.authz.cache;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * The request used to clear the cache for native roles stored in an index.
 */
public class ClearRolesCacheRequest extends BaseNodesRequest<ClearRolesCacheRequest> {

    String[] roles;

    /**
     * Sets the roles for which caches will be evicted. When not set all the roles will be evicted from the cache.
     *
     * @param roles    The role names
     */
    public ClearRolesCacheRequest roles(String... roles) {
        this.roles = roles;
        return this;
    }

    /**
     * @return an array of role names that will have the cache evicted or <code>null</code> if all
     */
    public String[] roles() {
        return roles;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        roles = in.readOptionalStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStringArray(roles);
    }

    public static class Node extends BaseNodeRequest {
        String[] roles;

        public Node() {
        }

        public Node(ClearRolesCacheRequest request, String nodeId) {
            super(nodeId);
            this.roles = request.roles();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            roles = in.readOptionalStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(roles);
        }
    }
}
