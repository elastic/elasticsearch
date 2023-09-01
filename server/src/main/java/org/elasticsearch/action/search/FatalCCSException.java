/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import java.util.Objects;

/**
 * Exception that indicates an error during a cross-cluster search is "fatal", meaning
 * that the search should be stopped immediately. It acts as a marker, holding the
 * underlying error. The getCause() method is guaranteed to be non-null.
 */
public class FatalCCSException extends RuntimeException {

    private final String clusterAlias;

    public FatalCCSException(String clusterAlias, Throwable cause) {
        super(cause);
        assert cause != null : "Cause should always be set on FatalCCSException";
        this.clusterAlias = Objects.requireNonNull(clusterAlias);
    }

    /**
     * @return alias of the cluster that had the fatal underlying exception.
     *         Guaranteed to not be null. The local cluster will have the alias of
     *         RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.
     */
    public String getClusterAlias() {
        return clusterAlias;
    }
}
