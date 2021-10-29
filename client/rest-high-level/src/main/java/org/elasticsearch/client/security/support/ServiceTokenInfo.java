/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support;

import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.Objects;

public class ServiceTokenInfo {
    private final String name;
    private final String source;
    @Nullable
    private final Collection<String> nodeNames;

    public ServiceTokenInfo(String name, String source) {
        this(name, source, null);
    }

    public ServiceTokenInfo(String name, String source, Collection<String> nodeNames) {
        this.name = Objects.requireNonNull(name, "token name is required");
        this.source = Objects.requireNonNull(source, "token source is required");
        this.nodeNames = nodeNames;
    }

    public String getName() {
        return name;
    }

    public String getSource() {
        return source;
    }

    public Collection<String> getNodeNames() {
        return nodeNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServiceTokenInfo that = (ServiceTokenInfo) o;
        return Objects.equals(name, that.name) && Objects.equals(source, that.source) && Objects.equals(nodeNames, that.nodeNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, source, nodeNames);
    }

    @Override
    public String toString() {
        return "ServiceTokenInfo{" + "name='" + name + '\'' + ", source='" + source + '\'' + ", nodeNames=" + nodeNames + '}';
    }
}
