/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Objects;

/** Per-cluster metadata of an ES|QL query, as reported under {@code _clusters} in the REST response. */
public record EsqlClusterInfo(String clusterAlias, @Nullable String indexExpression, Status status) {

    /** The alias of the local cluster. */
    public static final String LOCAL_CLUSTER_ALIAS = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    /** The status of the query on a cluster. */
    public enum Status {
        RUNNING,
        SUCCESSFUL,
        PARTIAL,
        SKIPPED,
        FAILED
    }

    public EsqlClusterInfo {
        Objects.requireNonNull(clusterAlias, "clusterAlias");
        Objects.requireNonNull(status, "status");
    }
}
