/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Cluster-specific deprecation checks, this is used to populate the {@code cluster_settings} field
 */
public class ClusterDeprecationChecker {

    private static final Logger logger = LogManager.getLogger(ClusterDeprecationChecker.class);
    private final List<TriConsumer<ClusterState, List<TransformConfig>, List<DeprecationIssue>>> CHECKS = List.of(
        this::checkTransformSettings
    );
    private final NamedXContentRegistry xContentRegistry;

    ClusterDeprecationChecker(NamedXContentRegistry xContentRegistry) {
        this.xContentRegistry = xContentRegistry;
    }

    public List<DeprecationIssue> check(ClusterState clusterState, List<TransformConfig> transformConfigs) {
        List<DeprecationIssue> allIssues = new ArrayList<>();
        CHECKS.forEach(check -> check.apply(clusterState, transformConfigs, allIssues));
        return allIssues;
    }

    private void checkTransformSettings(
        ClusterState clusterState,
        List<TransformConfig> transformConfigs,
        List<DeprecationIssue> allIssues
    ) {
        for (var config : transformConfigs) {
            try {
                allIssues.addAll(config.checkForDeprecations(xContentRegistry));
            } catch (IOException e) {
                logger.warn("failed to check transformation settings for '" + config.getId() + "'", e);
            }
        }
    }
}
