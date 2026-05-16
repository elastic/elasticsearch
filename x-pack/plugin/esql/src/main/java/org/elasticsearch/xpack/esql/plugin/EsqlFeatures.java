/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.Build;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.rest.action.admin.cluster.RestNodesCapabilitiesAction;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

import java.util.Set;

/**
 * {@link NodeFeature}s declared by ESQL. These should be used for fast checks
 * on the node. Before the introduction of the {@link RestNodesCapabilitiesAction}
 * this was used for controlling which features are tested so many of the
 * examples below are *just* used for that. Don't make more of those - add them
 * to {@link EsqlCapabilities} instead.
 * <p>
 *     NOTE: You can only remove features on major version boundaries.
 *     Only add more of these if you need a fast CPU level check.
 * </p>
 */
public class EsqlFeatures implements FeatureSpecification {
    /**
     * Support metrics syntax
     */
    public static final NodeFeature METRICS_SYNTAX = new NodeFeature("esql.metrics_syntax");

    @Override
    public Set<NodeFeature> getFeatures() {
        if (Build.current().isSnapshot()) {
            return Set.of(METRICS_SYNTAX);
        } else {
            return Set.of();
        }
    }
}
