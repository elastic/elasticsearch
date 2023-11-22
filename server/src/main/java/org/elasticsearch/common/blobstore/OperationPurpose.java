/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.Strings;

/**
 * The purpose of an operation against the blobstore. For example, it can be useful for stats collection
 * as well as other things that requires further differentiation for the same blob operation.
 */
public enum OperationPurpose {
    SNAPSHOT("Snapshot"),
    REPOSITORY_ANALYSIS("RepositoryAnalysis"),
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

    public static OperationPurpose parse(String s) {
        for (OperationPurpose purpose : OperationPurpose.values()) {
            if (purpose.key.equals(s)) {
                return purpose;
            }
        }
        throw new IllegalArgumentException(
            Strings.format("invalid purpose [%s] expected one of [%s]", s, Strings.arrayToCommaDelimitedString(OperationPurpose.values()))
        );
    }
}
