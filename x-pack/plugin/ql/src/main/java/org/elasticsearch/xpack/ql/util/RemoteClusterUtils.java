/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.core.Tuple;

import java.util.StringJoiner;

import static org.elasticsearch.transport.RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;
import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public class RemoteClusterUtils {

    public static Tuple<String, String> splitQualifiedIndex(String indexName) {
        int separatorOffset = indexName.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR);
        return separatorOffset > 0
            ? Tuple.tuple(indexName.substring(0, separatorOffset), indexName.substring(separatorOffset + 1))
            : Tuple.tuple(null, indexName);
    }

    public static String qualifyAndJoinIndices(String cluster, String[] indices) {
        StringJoiner sj = new StringJoiner(",");
        for (String index : indices) {
            sj.add(cluster != null ? buildRemoteIndexName(cluster, index) : index);
        }
        return sj.toString();
    }

    public static boolean isQualified(String indexWildcard) {
        return indexWildcard.indexOf(REMOTE_CLUSTER_INDEX_SEPARATOR) > 0;
    }
}
