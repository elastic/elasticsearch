/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support;

import java.util.Objects;

public class ServiceTokenInfo {
    private final String name;
    private final String source;

    public ServiceTokenInfo(String name, String source) {
        this.name = Objects.requireNonNull(name, "token name is required");
        this.source = Objects.requireNonNull(source, "token source is required");
    }

    public String getName() {
        return name;
    }

    public String getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ServiceTokenInfo that = (ServiceTokenInfo) o;
        return name.equals(that.name) && source.equals(that.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, source);
    }

    @Override
    public String toString() {
        return "ServiceTokenInfo{" + "name='" + name + '\'' + ", source='" + source + '\'' + '}';
    }
}
