/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.resourceexhaustion;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

/**
 * Base class for resource exhaustion tests that run against a single node with a constrained heap.
 * Use this for tests where the node doing the work is also the one enforcing limits — circuit
 * breakers on fielddata, request memory, and thread pool saturation.
 */
public abstract class ResourceExhaustionSingleNodeTestCase extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .setting("xpack.security.enabled", "false")
        // Allow test setup to stream large payloads for indexing. This is an HTTP transport
        // limit unrelated to the memory circuit breakers under test.
        .setting("http.max_content_length", "300mb")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
