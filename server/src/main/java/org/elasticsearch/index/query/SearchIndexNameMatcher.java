/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.function.Predicate;

/**
 * A predicate that checks whether an index pattern matches the current search shard target.
 */
public class SearchIndexNameMatcher implements Predicate<String> {
    private final String indexName;
    private final String clusterAlias;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver expressionResolver;

    /**
     * Creates a new index name matcher.
     *
     * @param indexName he name of the local index.
     * @param clusterAlias the cluster alias of this search shard target. If it is a local target, the alias
     *                     should be null or equal to {@link RemoteClusterAware#LOCAL_CLUSTER_GROUP_KEY}.
     * @param clusterService the cluster service.
     */
    public SearchIndexNameMatcher(String indexName,
                                  String clusterAlias,
                                  ClusterService clusterService,
                                  IndexNameExpressionResolver expressionResolver) {
        this.indexName = indexName;
        this.clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(clusterAlias) ? null : clusterAlias;
        this.clusterService = clusterService;
        this.expressionResolver = expressionResolver;
    }

    /**
     *  Given an index pattern, checks whether it matches against the current shard.
     *
     *  If this shard represents a remote shard target, then in order to match the pattern contain
     *  the separator ':', and must match on both the cluster alias and index name.
     */
    public boolean test(String pattern) {
        int separatorIndex = pattern.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
        if (separatorIndex < 0) {
            return clusterAlias == null && matchesIndex(pattern);
        } else {
            String clusterPattern = pattern.substring(0, separatorIndex);
            String indexPattern = pattern.substring(separatorIndex + 1);

            return Regex.simpleMatch(clusterPattern, clusterAlias) && matchesIndex(indexPattern);
        }
    }

    private boolean matchesIndex(String pattern) {
        String[] concreteIndices = expressionResolver.concreteIndexNames(
            clusterService.state(), IndicesOptions.lenientExpandOpen(), true, pattern);
        for (String index : concreteIndices) {
            if (Regex.simpleMatch(index, indexName)) {
                return true;
            }
        }
        return false;
    }
}
