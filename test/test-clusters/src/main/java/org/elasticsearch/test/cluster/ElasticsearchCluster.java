/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.local.DefaultLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.junit.rules.TestRule;

/**
 * <p>A JUnit test rule for orchestrating an Elasticsearch cluster for local integration testing. New clusters can be created via one of the
 * various static builder methods. For example:</p>
 * <pre>
 * &#064;ClassRule
 * public static ElasticsearchCluster myCluster = ElasticsearchCluster.local().build();
 * </pre>
 */
public interface ElasticsearchCluster extends TestRule, ClusterHandle {

    /**
     * Creates a new {@link LocalClusterSpecBuilder} for defining a locally orchestrated cluster. Local clusters use a locally built
     * Elasticsearch distribution.
     *
     * @return a builder for a local cluster
     */
    static LocalClusterSpecBuilder<ElasticsearchCluster> local() {
        return new DefaultLocalClusterSpecBuilder();
    }

}
