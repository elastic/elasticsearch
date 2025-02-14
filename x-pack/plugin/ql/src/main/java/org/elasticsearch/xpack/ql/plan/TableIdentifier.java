/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan;

import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

import static org.elasticsearch.cluster.metadata.IndexNameExpressionResolver.SelectorResolver.SELECTOR_SEPARATOR;
import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;

public class TableIdentifier {

    private final Source source;

    private final String cluster;
    private final String index;
    private final String selector;

    public TableIdentifier(Source source, String catalog, String index, String selector) {
        this.source = source;
        this.cluster = catalog;
        this.index = index;
        this.selector = selector;
    }

    public String cluster() {
        return cluster;
    }

    public String index() {
        return index;
    }

    public String selector() {
        return selector;
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, index, selector);
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
        return Objects.equals(index, other.index) && Objects.equals(cluster, other.cluster) && Objects.equals(selector, other.selector);
    }

    public Source source() {
        return source;
    }

    public String qualifiedIndex() {
        if (cluster == null && selector == null) {
            return index;
        }
        StringBuilder qualifiedIndex = new StringBuilder();
        if (cluster != null) {
            qualifiedIndex.append(cluster);
            qualifiedIndex.append(REMOTE_CLUSTER_INDEX_SEPARATOR);
        }
        qualifiedIndex.append(index);
        if (selector != null) {
            qualifiedIndex.append(SELECTOR_SEPARATOR);
            qualifiedIndex.append(selector);
        }
        return qualifiedIndex.toString();
    }

    @Override
    public String toString() {
        return qualifiedIndex();
    }
}
