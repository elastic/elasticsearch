/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public class EsIndex {

    @Nullable
    private final String cluster;
    private final String name;
    private final Map<String, EsField> mapping;

    public EsIndex(String name, Map<String, EsField> mapping) {
        this(null, name, mapping);
    }

    public EsIndex(@Nullable String cluster, String name, Map<String, EsField> mapping) {
        assert name != null;
        assert mapping != null;
        this.cluster = cluster;
        this.name = name;
        this.mapping = mapping;
    }

    public String cluster() {
        return cluster;
    }

    public String name() {
        return name;
    }

    public String qualifiedName() {
        return buildRemoteIndexName(cluster, name);
    }

    public Map<String, EsField> mapping() {
        return mapping;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, name, mapping);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsIndex other = (EsIndex) obj;
        return Objects.equals(cluster, other.cluster) && Objects.equals(name, other.name) && mapping == other.mapping;
    }
}
