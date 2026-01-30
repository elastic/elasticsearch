/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

public class ClearSecurityCacheRequest extends BaseNodesRequest {

    private String cacheName;
    private String[] keys;

    public ClearSecurityCacheRequest() {
        super((String[]) null);
    }

    public ClearSecurityCacheRequest cacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    public String cacheName() {
        return cacheName;
    }

    public ClearSecurityCacheRequest keys(String... keys) {
        this.keys = keys;
        return this;
    }

    public String[] keys() {
        return keys;
    }

    public static class Node extends AbstractTransportRequest {
        private String cacheName;
        private String[] keys;

        public Node(StreamInput in) throws IOException {
            super(in);
            cacheName = in.readString();
            keys = in.readOptionalStringArray();
        }

        public Node(ClearSecurityCacheRequest request) {
            this.cacheName = request.cacheName();
            this.keys = request.keys();
        }

        public String getCacheName() {
            return cacheName;
        }

        public String[] getKeys() {
            return keys;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(cacheName);
            out.writeOptionalStringArray(keys);
        }
    }
}
