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
 * The single 3-node cluster shared by every IT class in this project.
 * <p>
 * The legacy {@code testClusters} configuration created one cluster per {@code javaRestTest} task, so all IT
 * classes ran against the same nodes. {@code shared(true)} preserves that behaviour rather than starting a
 * fresh 3-node cluster per class. Test isolation still holds because {@code ESRestTestCase} wipes cluster
 * content after every test method, including a {@code POST /_features/_reset} that clears ML jobs, datafeeds
 * and trained models.
 * <p>
 * Two consequences of sharing, both wired up deliberately:
 * <ul>
 *     <li>Every class using this cluster must carry
 *     {@code @ThreadLeakFilters(filters = TestClustersThreadFilter.class)}; the framework asserts this because a
 *     shared cluster outlives the suite that started it, so its threads would be reported as suite-level leaks.</li>
 *     <li>{@code javaRestTest} pins {@code maxParallelForks = 1} in {@code build.gradle}. Sharing is per-JVM, so
 *     parallel forks would each start their own 3-node cluster and negate the benefit.</li>
 * </ul>
 * The default distribution is required, not merely convenient: {@code xpack.monitoring.*}, {@code slm.*} and
 * {@code indices.lifecycle.*} settings below are only registered when those modules are present, and the ML
 * index templates reference the ILM-owned {@code index.lifecycle.name} setting.
 */
public final class Clusters {

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
