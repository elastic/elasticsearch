/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan;

import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;

public class TableIdentifier {

    private final Source source;

    private final String cluster;
    private final String index;

    public TableIdentifier(Source source, String catalog, String index) {
        this.source = source;
        this.cluster = catalog;
        this.index = index;
    }

    public String cluster() {
        return cluster;
    }

    public String index() {
        return index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, index);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TableIdentifier other = (TableIdentifier) obj;
        return Objects.equals(index, other.index) && Objects.equals(cluster, other.cluster);
    }

    public Source source() {
        return source;
    }

    public String qualifiedIndex() {
        return cluster != null ? cluster + REMOTE_CLUSTER_INDEX_SEPARATOR + index : index;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (cluster != null) {
            builder.append(cluster);
            builder.append(REMOTE_CLUSTER_INDEX_SEPARATOR);
        }
        builder.append(index);
        return builder.toString();
    }
}
