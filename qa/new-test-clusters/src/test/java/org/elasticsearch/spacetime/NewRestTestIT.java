/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.spacetime;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;

public class NewRestTestIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .name("my-cluster")
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.security.enabled", "false")
        .withNode(s -> s.name("node-0"))
        .withNode(s -> s.name("node-1"))
        .withNode(s -> s.name("node-2"))
        .build();

    public void testSomething() throws IOException {
        createIndex("my-index");
        assertFalse(true);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
