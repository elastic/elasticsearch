/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.realm;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ClearRealmCacheRequest extends BaseNodesRequest<ClearRealmCacheRequest> {

    String[] realms;
    String[] usernames;

    public ClearRealmCacheRequest() {
        super((String[]) null);
    }

    public ClearRealmCacheRequest(StreamInput in) throws IOException {
        super(in);
        realms = in.readStringArray();
        usernames = in.readStringArray();
    }

    /**
     * @return  {@code true} if this request targets realms, {@code false} otherwise.
     */
    public boolean allRealms() {
        return realms == null || realms.length == 0;
    }

    /**
     * @return  The realms that should be evicted. Empty array indicates all realms.
     */
    public String[] realms() {
        return realms;
    }

    /**
     * Sets the realms for which caches will be evicted. When not set all the caches of all realms will be
     * evicted.
     *
     * @param realms    The realm names
     */
    public ClearRealmCacheRequest realms(String... realms) {
        this.realms = realms;
        return this;
    }

    /**
     * @return  The usernames of the users that should be evicted. Empty array indicates all users.
     */
    public String[] usernames() {
        return usernames;
    }

    /**
     * Sets the usernames of the users that should be evicted from the caches. When not set, all users
     * will be evicted.
     *
     * @param usernames The usernames
     */
    public ClearRealmCacheRequest usernames(String... usernames) {
        this.usernames = usernames;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArrayNullable(realms);
        out.writeStringArrayNullable(usernames);
    }

    public static class Node extends TransportRequest {

        private final String[] realms;
        private final String[] usernames;

        public Node(StreamInput in) throws IOException {
            super(in);
            realms = in.readStringArray();
            usernames = in.readStringArray();
        }

        public Node(ClearRealmCacheRequest request) {
            this.realms = request.realms;
            this.usernames = request.usernames;
        }

        public String[] getRealms() {
            return realms;
        }

        public String[] getUsernames() {
            return usernames;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArrayNullable(realms);
            out.writeStringArrayNullable(usernames);
        }
    }
}
