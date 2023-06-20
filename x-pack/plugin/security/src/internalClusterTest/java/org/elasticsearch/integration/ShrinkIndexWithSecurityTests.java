/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Integration test that uses multiple data nodes to test that the shrink index api works with security.
 */
@ClusterScope(minNumDataNodes = 2)
public class ShrinkIndexWithSecurityTests extends SecurityIntegTestCase {

    @Override
    protected final boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }

    public void testShrinkIndex() throws Exception {
        final int randomNumberOfDocs = scaledRandomIntBetween(2, 12);
        for (int i = 0; i < randomNumberOfDocs; i++) {
            client().prepareIndex("bigindex").setSource("foo", "bar").get();
        }

        Map<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(DiscoveryNode[]::new);
        final String mergeNode = discoveryNodes[0].getName();
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        updateIndexSettings(
            Settings.builder().put("index.routing.allocation.require._name", mergeNode).put("index.blocks.write", true),
            "bigindex"
        );

        // wait for green and then shrink
        ensureGreen();
        assertAcked(indicesAdmin().prepareResizeIndex("bigindex", "shrunk_bigindex").setSettings(indexSettings(1, 0).build()));

        // verify all docs
        ensureGreen();
        assertHitCount(
            client().prepareSearch("shrunk_bigindex").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
            randomNumberOfDocs
        );
    }
}
