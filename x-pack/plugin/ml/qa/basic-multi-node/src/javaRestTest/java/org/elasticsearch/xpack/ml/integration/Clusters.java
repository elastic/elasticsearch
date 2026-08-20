/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

/**
 * The single 3-node cluster shared by every IT class in this project. The legacy {@code testClusters}
 * configuration created one cluster per {@code javaRestTest} task, so all classes ran against the same nodes;
 * {@code shared(true)} preserves that behaviour and avoids paying the 3-node start-up cost per class.
 */
public abstract class Clusters {

    public static final ElasticsearchCluster CLUSTER = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(3)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.monitoring.elasticsearch.collection.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.ml.enabled", "true")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("indices.lifecycle.history_index_enabled", "false")
        .setting("slm.history_index_enabled", "false")
        .shared(true)
        .build();

    private Clusters() {}
}
