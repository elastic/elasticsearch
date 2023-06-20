/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoAction;
import org.elasticsearch.action.admin.cluster.remote.RemoteInfoRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.transport.RemoteConnectionInfo;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class CrossClusterReindexIT extends AbstractMultiClustersTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CrossClusterReindexIT.class);

    private static final String REMOTE_CLUSTER = "remote-cluster";

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected Collection<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(ReindexPlugin.class);
    }

    @Override
    protected void configureRemoteCluster(String clusterAlias, Collection<String> seedNodes) throws Exception {
        final String remoteClusterSettingPrefix = "cluster.remote." + clusterAlias + ".";
        Settings.Builder settings = Settings.builder();
        final List<String> seedAddresses = seedNodes.stream().map(node -> {
            final TransportService transportService = cluster(clusterAlias).getInstance(TransportService.class, node);
            return transportService.boundAddress().publishAddress().toString();
        }).toList();

        LOGGER.info("--> use sniff mode with seed [{}], remote nodes [{}]", Collectors.joining(","), seedNodes);
        settings.putNull(remoteClusterSettingPrefix + "proxy_address")
            .put(remoteClusterSettingPrefix + "mode", "sniff")
            .put(remoteClusterSettingPrefix + "seeds", String.join(",", seedAddresses))
            .put(remoteClusterSettingPrefix + "skip_unavailable", true)
            .build();
        client(LOCAL_CLUSTER).admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();

        assertBusy(() -> {
            List<RemoteConnectionInfo> remoteConnectionInfos = client(LOCAL_CLUSTER).execute(
                RemoteInfoAction.INSTANCE,
                new RemoteInfoRequest()
            )
                .actionGet()
                .getInfos()
                .stream()
                .filter(c -> c.isConnected() && c.getClusterAlias().equals(clusterAlias))
                .collect(Collectors.toList());
            assertThat(remoteConnectionInfos, not(empty()));
        });
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    public void testReindexFromRemote() throws Exception {
        assertAcked(client(REMOTE_CLUSTER).admin().indices().prepareCreate("source-index-001"));
        final int docsNumber = indexDocs(client(REMOTE_CLUSTER), "source-index-001");

        final String sourceIndexInRemote = REMOTE_CLUSTER + ":" + "source-index-001";
        new ReindexRequestBuilder(client(LOCAL_CLUSTER), ReindexAction.INSTANCE).source(sourceIndexInRemote)
            .destination("desc-index-001")
            .get();

        assertTrue("Number of documents in source and desc indexes does not match", waitUntil(() -> {
            SearchResponse resp_dest = client(LOCAL_CLUSTER).prepareSearch("desc-index-001")
                .setQuery(new MatchAllQueryBuilder())
                .setSize(1000)
                .get();
            final TotalHits totalHits = resp_dest.getHits().getTotalHits();
            return totalHits.relation == TotalHits.Relation.EQUAL_TO && totalHits.value == docsNumber;
        }));
    }
}
