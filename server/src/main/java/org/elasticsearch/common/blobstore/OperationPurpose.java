/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

/**
 * The purpose of an operation against the blobstore. For example, it can be useful for stats collection
 * as well as other things that requires further differentiation for the same blob operation.
 */
public enum OperationPurpose {
    SNAPSHOT("Snapshot"),
    CLUSTER_STATE("ClusterState"),
    INDICES("Indices"),
    TRANSLOG("Translog");

    private final String key;

    OperationPurpose(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
