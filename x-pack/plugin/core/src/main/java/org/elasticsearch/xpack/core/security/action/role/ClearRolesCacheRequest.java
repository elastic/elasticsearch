/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.support.nodes.BaseNodeRequest;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * The request used to clear the cache for native roles stored in an index.
 */
public class ClearRolesCacheRequest extends BaseNodesRequest<ClearRolesCacheRequest> {

    String[] names;

    public ClearRolesCacheRequest() {
        super((String[]) null);
    }

    public ClearRolesCacheRequest(StreamInput in) throws IOException {
        super(in);
        names = in.readOptionalStringArray();
    }
    /**
     * Sets the roles for which caches will be evicted. When not set all the roles will be evicted from the cache.
     *
     * @param names    The role names
     */
    public ClearRolesCacheRequest names(String... names) {
        this.names = names;
        return this;
    }

    /**
     * @return an array of role names that will have the cache evicted or <code>null</code> if all
     */
    public String[] names() {
        return names;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalStringArray(names);
    }

    public static class Node extends BaseNodeRequest {
        private String[] names;

        public Node(StreamInput in) throws IOException {
            super(in);
            names = in.readOptionalStringArray();
        }

        public Node(ClearRolesCacheRequest request) {
            this.names = request.names();
        }

        public String[] getNames() { return names; }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalStringArray(names);
        }
    }
}
