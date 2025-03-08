/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;

import java.util.List;
import java.util.Map;

/**
 * The resource deprecation checker allows us to extend the deprecation API by adding deprecation checks for resources. It differs
 * from the {@link DeprecationChecker} because it groups the deprecation issues by a single resource, not only by type. For example,
 * the "data_streams" checker will contain a map from data stream name its deprecation issues.
 */
public interface ResourceDeprecationChecker {

    /**
     * This runs the checks for the current deprecation checker.
     *
     * @param clusterState The cluster state provided for the checker
     * @param request The deprecation request that triggered this check
     * @param precomputedData Data that have been remotely retrieved and might be useful in the checks
     */
    Map<String, List<DeprecationIssue>> check(
        ClusterState clusterState,
        DeprecationInfoAction.Request request,
        TransportDeprecationInfoAction.PrecomputedData precomputedData
    );

    /**
     * @return The name of the checker
     */
    String getName();
}
