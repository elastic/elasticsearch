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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;

import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class SearchShardRelocationIT extends AbstractStatelessIntegTestCase {

    public void testSearchShardRelocationWhenHostingNodeTerminates() throws Exception {
        // MAX_MISSED_HEARTBEATS x HEARTBEAT_FREQUENCY is how long it takes for the last master heartbeat to expire.
        // Speed up the time to master election after master restart.
        final Settings nodeSettings = Settings.builder()
            .put(HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 2)
            .build();
        var indexNode = startMasterAndIndexNode(nodeSettings);
        var searchNodeA = startSearchNode(nodeSettings);
        var searchNodeB = startSearchNode(nodeSettings);

        var indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        int numDocs = randomIntBetween(1, 100);
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);

        var searchNodeCurrent = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeA : searchNodeB;
        var searchNodeNext = internalCluster().nodesInclude(indexName).contains(searchNodeA) ? searchNodeB : searchNodeA;

        if (rarely()) {
            internalCluster().restartNode(indexNode);
        }
        internalCluster().stopNode(searchNodeCurrent);
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), not(hasItem(searchNodeCurrent)));
        assertThat(internalCluster().nodesInclude(indexName), hasItem(searchNodeNext));
    }
}
