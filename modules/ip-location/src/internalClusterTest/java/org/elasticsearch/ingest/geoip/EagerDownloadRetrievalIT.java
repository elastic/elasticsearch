/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

/**
 * Regression test for the interaction between {@code ingest.geoip.downloader.eager.download} and node-local
 * database retrieval. The geoip downloader and the per-node {@code DatabaseNodeService} were split into separate
 * concerns, after which eager download only populated the {@code .geoip_databases} index while node-local retrieval
 * was gated behind a registered {@code IpLocationConsumer} (i.e. an actual geoip pipeline). That made the per-node
 * {@code databases} reported by {@code _ingest/geoip/stats} stay empty under eager download until something started
 * ingesting, which in turn caused the first ingested document to be tagged {@code _<type>_database_unavailable_<db>}.
 *
 * <p>The cluster is built with an explicit mixed-role topology — one master+data+ingest node and one data-only
 * node — so both halves of the {@code isIngestNode()} guard are pinned deterministically: eager download must
 * retrieve the databases on the ingest node and must <em>not</em> retrieve them on the (non-ingest) data-only node.
 * A data-only node is the meaningful negative here: it is the node that would retrieve databases under an ESQL
 * consumer, so it staying empty shows eager retrieval is specifically ingest-scoped. It runs in its own
 * {@link ESIntegTestCase.Scope#TEST}-scoped cluster so toggling the (dynamic) eager setting cannot leak into other
 * suites.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EagerDownloadRetrievalIT extends AbstractGeoIpIT {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, IngestGeoIpPlugin.class, IngestGeoIpSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (getEndpoint() != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getEndpoint());
        }
        return settings.build();
    }

    public void testEagerDownloadRetrievesDatabasesWithoutPipeline() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        ProjectId projectId = ProjectId.DEFAULT;

        // Explicit mixed-role topology: the first node is master-eligible so the cluster can form, and is ingest-capable
        // so it should retrieve under eager download; the second is data-only so it should not.
        internalCluster().startNode(
            onlyRoles(Settings.EMPTY, Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE))
        );
        internalCluster().startNode(onlyRoles(Settings.EMPTY, Set.of(DiscoveryNodeRole.DATA_ROLE)));

        // No consumer is ever registered (no requestDownloads, no pipeline). Enabling eager download dynamically must
        // make the downloader fetch into the index AND make ingest-capable nodes retrieve and load the databases
        // locally. Setting the dynamic setting at runtime also exercises the settings-update-consumer wiring in
        // IngestGeoIpPlugin.
        updateClusterSettings(
            Settings.builder()
                .put(GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getKey(), true)
                .put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true)
        );

        assertBusy(() -> {
            // Sanity check: no IpLocationConsumer was registered, so retrieval here is driven purely by eager download.
            IpLocationDownloadConsumers consumers = clusterService().state()
                .metadata()
                .getProject(projectId)
                .custom(IpLocationDownloadConsumers.TYPE);
            assertTrue(
                "no IpLocationConsumer should be registered when relying on eager download",
                consumers == null || consumers.hasConsumers() == false
            );

            GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
            assertThat(response.getNodes(), not(empty()));
            for (GeoIpStatsAction.NodeResponse nodeResponse : response.getNodes()) {
                DiscoveryNode node = nodeResponse.getNode();
                // getDatabases() reports locally-retrieved (downloaded) databases, distinct from config databases.
                if (node.isIngestNode()) {
                    assertThat(
                        "ingest node [" + node.getName() + "] should retrieve databases under eager download",
                        nodeResponse.getDatabases(),
                        hasItems("GeoLite2-City.mmdb", "GeoLite2-Country.mmdb", "GeoLite2-ASN.mmdb")
                    );
                } else {
                    assertThat(
                        "non-ingest node [" + node.getName() + "] should not retrieve databases under eager download",
                        nodeResponse.getDatabases(),
                        empty()
                    );
                }
            }
            // Generous relative to the observed sub-second download+retrieve against the fixture, but bounded so a real
            // regression (databases never retrieved locally) fails fast instead of hanging.
        }, 30, TimeUnit.SECONDS);
    }
}
