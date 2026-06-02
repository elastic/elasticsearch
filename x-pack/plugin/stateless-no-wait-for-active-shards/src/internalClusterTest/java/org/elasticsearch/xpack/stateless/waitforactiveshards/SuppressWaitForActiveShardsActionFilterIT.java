/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.waitforactiveshards;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collection;
import java.util.function.UnaryOperator;

public class SuppressWaitForActiveShardsActionFilterIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), NoWaitForActiveShardsPlugin.class);
    }

    public void testIndexingIgnoresWaitForActiveShards() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        final var searchNode = startSearchNode();
        final var indexName = randomIdentifier();
        createIndex(indexName, 1, 1);

        final UnaryOperator<BulkRequestBuilder> maybeSetWaitForActiveShards = bulkRequest -> {
            if (randomBoolean()) {
                bulkRequest.setWaitForActiveShards(
                    randomFrom(
                        ActiveShardCount.ALL,
                        ActiveShardCount.NONE,
                        ActiveShardCount.DEFAULT,
                        ActiveShardCount.ONE,
                        new ActiveShardCount(between(0, 5))
                    )
                );
                bulkRequest.setTimeout(TimeValue.timeValueSeconds(10));
            }
            return bulkRequest;
        };

        for (int i = 0; i < 10; i++) {
            indexDocs(indexName, between(1, 10), maybeSetWaitForActiveShards);
        }
        ensureGreen(indexName);

        internalCluster().stopNode(searchNode);
        for (int i = 0; i < 10; i++) {
            indexDocs(indexName, between(1, 10), maybeSetWaitForActiveShards);
        }
    }
}
