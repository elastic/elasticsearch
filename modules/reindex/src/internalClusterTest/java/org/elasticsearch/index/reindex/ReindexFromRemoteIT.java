/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/// Integration tests for reindex-from-remote that route through the cluster's own HTTP endpoint as the "remote" source.
///
/// For tests of remote reindex behaviour that *do* require an actual older Elasticsearch fixture (BWC traffic against
/// 7.x remotes), see `ReindexFromRemote7xIT` in the `javaRestTest` sources.
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class ReindexFromRemoteIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {

        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    public void testTotalIsAccurateWhenSourceExceedsDefaultTrackTotalHitsCap() {
        int numDocs = SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO + randomIntBetween(1, SearchContext.DEFAULT_TRACK_TOTAL_HITS_UP_TO);
        indexRandom(true, "source", numDocs);
        // The verification searches must opt into accurate total tracking themselves; otherwise they hit the same default
        // cap we are testing reindex's behaviour around.
        assertHitCount(prepareSearch("source").setTrackTotalHits(true).setSize(0), numDocs);

        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        RemoteInfo remote = new RemoteInfo(
            "http",
            remoteAddress.getHostString(),
            remoteAddress.getPort(),
            null,
            new BytesArray("{\"match_all\":{}}"),
            null,
            null,
            Map.of(),
            RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
            RemoteInfo.DEFAULT_CONNECT_TIMEOUT
        );

        BulkByScrollResponse response = new ReindexRequestBuilder(client()).source("source")
            .destination("dest")
            .setRemoteInfo(remote)
            .refresh(true)
            .get();

        assertEquals(numDocs, response.getStatus().getTotal());
        assertEquals(numDocs, response.getCreated());
        assertHitCount(prepareSearch("dest").setTrackTotalHits(true).setSize(0), numDocs);
    }
}
