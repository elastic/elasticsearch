/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql;

import org.apache.http.HttpHost;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;

public record TestNode(String id, Version version, @Nullable TransportVersion transportVersion, HttpHost publishAddress) {
    @Override
    public String toString() {
        return "Node{" + "id='" + id + '\'' + ", version=" + version + '}';
    }
}
