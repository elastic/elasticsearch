/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.ccs;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Base class for cross-cluster search (CCS) integration tests.
 * Presumed 2-node cluster, with remote called REMOTE_CLUSTER.
 */
public abstract class AbstractCrossClusterSearchTestCase extends AbstractMultiClustersTestCase {

    protected static final String REMOTE_CLUSTER = "cluster_a";
    protected static final long EARLIEST_TIMESTAMP = 1691348810000L;
    protected static final long LATEST_TIMESTAMP = 1691348820000L;

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER, randomBoolean());
    }

    @Override
    protected boolean reuseClusters() {
        // Don't reuse by default, most test suites are kinda nasty...
        return false;
    }

    /**
     * Set up local and remote indices with the same mapping and timestamped docs.
     * Returns a map with "local.num_shards", "remote.num_shards", "remote.skip_unavailable",
     * and optionally "local.index" / "remote.index" when using the no-arg overload.
     */
    protected Map<String, Object> setupTwoClusters(String[] localIndices, String[] remoteIndices) {
        int numShardsLocal = randomIntBetween(2, 10);
        Settings localSettings = indexSettings(numShardsLocal, randomIntBetween(0, 1)).build();
        for (String localIndex : localIndices) {
            assertAcked(
                client(LOCAL_CLUSTER).admin()
                    .indices()
                    .prepareCreate(localIndex)
                    .setSettings(localSettings)
                    .setMapping("@timestamp", "type=date", "f", "type=text")
            );
            indexDocsWithTimestamp(client(LOCAL_CLUSTER), localIndex);
        }

        int numShardsRemote = randomIntBetween(2, 10);
        final InternalTestCluster remoteCluster = cluster(REMOTE_CLUSTER);
        remoteCluster.ensureAtLeastNumDataNodes(randomIntBetween(1, 3));
        for (String remoteIndex : remoteIndices) {
            assertAcked(
                client(REMOTE_CLUSTER).admin()
                    .indices()
                    .prepareCreate(remoteIndex)
                    .setSettings(indexSettings(numShardsRemote, randomIntBetween(0, 1)))
                    .setMapping("@timestamp", "type=date", "f", "type=text")
            );
            assertFalse(
                client(REMOTE_CLUSTER).admin()
                    .cluster()
                    .prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex)
                    .setWaitForYellowStatus()
                    .setTimeout(TimeValue.timeValueSeconds(10))
                    .get()
                    .isTimedOut()
            );
            indexDocsWithTimestamp(client(REMOTE_CLUSTER), remoteIndex);
        }

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);
        return clusterInfo;
    }

    /**
     * Set up two clusters with default indices "demo" (local) and "prod" (remote).
     * Adds "local.index" and "remote.index" to the returned map.
     */
    protected Map<String, Object> setupTwoClusters() {
        Map<String, Object> clusterInfo = setupTwoClusters(new String[] { "demo" }, new String[] { "prod" });
        clusterInfo.put("local.index", "demo");
        clusterInfo.put("remote.index", "prod");
        return clusterInfo;
    }

    /**
     * Index documents with {@code f} and {@code @timestamp} fields for tests that rely on
     * a time range.
     */
    protected int indexDocsWithTimestamp(Client client, String index) {
        int numDocs = between(500, 1200);
        for (int i = 0; i < numDocs; i++) {
            long ts = EARLIEST_TIMESTAMP + i;
            if (i == numDocs - 1) {
                ts = LATEST_TIMESTAMP;
            }
            client.prepareIndex(index).setSource("f", "v", "@timestamp", ts).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    protected int indexDocs(Client client, String field, String index) {
        int numDocs = between(1, 200);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource(field, "v" + i).get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }
}
