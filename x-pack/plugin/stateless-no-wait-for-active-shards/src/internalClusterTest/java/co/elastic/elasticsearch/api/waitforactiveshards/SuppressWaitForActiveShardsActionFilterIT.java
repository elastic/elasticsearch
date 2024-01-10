/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.api.waitforactiveshards;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.function.UnaryOperator;

public class SuppressWaitForActiveShardsActionFilterIT extends AbstractStatelessIntegTestCase {

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
