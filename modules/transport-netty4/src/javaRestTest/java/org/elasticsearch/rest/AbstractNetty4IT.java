/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSystemPropertyProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

public abstract class AbstractNetty4IT extends ESRestTestCase {
    private static final MutableSystemPropertyProvider clusterSettings = new MutableSystemPropertyProvider();
    private final boolean usePooledAllocator;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("transport-netty4")
        .module("rest-root")
        .systemProperties(clusterSettings)
        .build();

    public AbstractNetty4IT(@Name("pooled") boolean pooledAllocator) {
        this.usePooledAllocator = pooledAllocator;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Before
    public void maybeRestart() throws IOException {
        // Restart the cluster to pick up the new setting if necessary
        String current = clusterSettings.get(null).get("es.use_unpooled_allocator");
        if (current == null || current.equals(Boolean.toString(usePooledAllocator))) {
            clusterSettings.get(null).put("es.use_unpooled_allocator", Boolean.toString(usePooledAllocator == false));
            cluster.restart(false);
            closeClients();
            initClient();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
