/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.ConfigurationException;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.StatelessReaderHeapBreaker;
import org.elasticsearch.xpack.stateless.utils.StatelessCommitServiceProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessPluginIT extends AbstractStatelessPluginIntegTestCase {

    public void testCreateStatelessCluster() throws Exception {
        if (randomBoolean()) {
            startMasterOnlyNode();
        } else {
            startMasterAndIndexNode();
        }
        final int numIndexNodes = randomIntBetween(0, 5);
        startIndexNodes(numIndexNodes);
        final int numSearchNodes = randomIntBetween(0, 5);
        startSearchNodes(numSearchNodes);
        ensureStableCluster(1 + numIndexNodes + numSearchNodes);
    }

    public void testCommitServiceOnlyInstantiatedOnIndexNodes() {
        final String indexNode = startMasterAndIndexNode();
        final String searchNode = startSearchNode();

        // StatelessCommitService must be present on index nodes
        assertThat(internalCluster().getInstance(StatelessCommitService.class, indexNode), notNullValue());
        assertThat(internalCluster().getInstance(StatelessCommitServiceProvider.class, indexNode).get(), notNullValue());

        // StatelessCommitService must be absent on search nodes
        assertThrows(AssertionError.class, () -> internalCluster().getInstance(StatelessCommitServiceProvider.class, searchNode).get());
        expectThrows(ConfigurationException.class, () -> internalCluster().getInstance(StatelessCommitService.class, searchNode));
    }

    public void testReaderHeapBreakerLimitIsDynamicallyUpdated() {
        startMasterAndIndexNode();
        final String searchNode = startSearchNode();

        final var breakerService = internalCluster().getInstance(CircuitBreakerService.class, searchNode);
        assertThat(breakerService.getBreaker(StatelessReaderHeapBreaker.NAME).getLimit(), equalTo(-1L));

        updateClusterSettings(Settings.builder().put(StatelessReaderHeapBreaker.LIMIT_SETTING.getKey(), "100mb"));

        final long expected = StatelessReaderHeapBreaker.LIMIT_SETTING.get(
            Settings.builder().put(StatelessReaderHeapBreaker.LIMIT_SETTING.getKey(), "100mb").build()
        ).getBytes();
        assertThat(
            "dynamic settings update must propagate to the live breaker limit",
            breakerService.getBreaker(StatelessReaderHeapBreaker.NAME).getLimit(),
            equalTo(expected)
        );
    }

}
