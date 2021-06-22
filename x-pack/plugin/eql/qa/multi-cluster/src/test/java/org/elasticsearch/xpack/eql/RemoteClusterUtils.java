/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

public class RemoteClusterUtils {
    public static final String REMOTE_CLUSTER_NAME = "my_remote_cluster";

    public static String qualifiedIndex(String indexName) {
        return REMOTE_CLUSTER_NAME + ":" + indexName;
    }

    public static String qualifiedPattern(String pattern) {
        StringBuilder sb = new StringBuilder();
        for (String index: pattern.split(",")) {
            sb.append(qualifiedIndex(index));
            sb.append(',');
        }
        return sb.substring(0, sb.length() - 1);
    }
}
