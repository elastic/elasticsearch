/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;

public final class TestNode {

    private final String id;
    private final Version version;
    private final HttpHost publishAddress;

    public TestNode(String id, Version version, HttpHost publishAddress) {
        this.id = id;
        this.version = version;
        this.publishAddress = publishAddress;
    }

    public String getId() {
        return id;
    }

    public HttpHost getPublishAddress() {
        return publishAddress;
    }

    public Version getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "Node{" + "id='" + id + '\'' + ", version=" + version + '}';
    }
}
