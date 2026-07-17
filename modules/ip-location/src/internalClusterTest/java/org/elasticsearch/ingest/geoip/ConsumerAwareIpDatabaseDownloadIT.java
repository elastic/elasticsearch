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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Integration tests verifying that IP database downloads are scoped to nodes
 * relevant to the consumer type that requested them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConsumerAwareIpDatabaseDownloadIT extends AbstractGeoIpIT {

    private static final TimeValue PHASE_TIMEOUT = TimeValue.timeValueMinutes(2);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ReindexPlugin.class,
            IngestGeoIpPlugin.class,
            IngestGeoIpSettingsPlugin.class,
            GeoIpIndexSettingProviderPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        if (getEndpoint() != null) {
            settings.put(GeoIpDownloader.ENDPOINT_SETTING.getKey(), getEndpoint());
        }
        return settings.build();
    }

    /**
     * Disable the downloader and wait for the persistent-tasks framework to remove the task entry, which in turn
     * triggers {@code GeoIpDownloaderTaskExecutor#deleteGeoIpDatabasesIndex} via the {@code onRemove} hook. Thanks
     * to the drain contract in {@link AbstractGeoIpDownloader} (cancellation defers {@code markAsCompleted()} until
     * any in-flight {@code runDownloader()} has returned), the DELETE runs against an idle index — no auto-create
     * race from a straggler bulk and therefore no shard left in {@code [starting shard]} state for
     * {@code InternalTestCluster#assertAfterTest} to time out on.
     * <p>
     * Guards against an empty cluster: if {@code assumeTrue} in the test body caused the test to be skipped before
     * any nodes were started, there is nothing to clean up and attempting to do so would throw
     * {@code AssertionError: Unable to get client, no node found}, which would turn the skip into a
     * {@code TestCouldNotBeSkippedException}.
     */
    @After
    public void cleanUp() throws Exception {
        if (internalCluster().size() == 0) {
            return;
        }
        updateClusterSettings(Settings.builder().putNull(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey()));
        assertBusy(
            () -> assertFalse(
                ".geoip_databases should be deleted by the onRemove hook after disabling the downloader",
                clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                    .get()
                    .getState()
                    .metadata()
                    .getProject(ProjectId.DEFAULT)
                    .getIndicesLookup()
                    .containsKey(GeoIpDownloader.DATABASES_INDEX)
            )
        );
    }

    /**
     * Verifies consumer-aware download scoping across a mixed-role cluster:
     * <ol>
     *   <li>INGEST consumer only: databases appear on ingest nodes, not on data-only nodes</li>
     *   <li>Adding ESQL consumer: databases also appear on data-only nodes</li>
     *   <li>Cancelling INGEST: databases remain on data-only nodes (ESQL still active)</li>
     *   <li>Cancelling ESQL: databases are removed from data-only nodes</li>
     * </ol>
     */
    public void testConsumerScopedDownloads() throws Exception {
        assumeTrue("only test with fixture to have stable results", getEndpoint() != null);
        String projectId = ProjectId.DEFAULT.id();

        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startNode(onlyRoles(Settings.EMPTY, Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.INGEST_ROLE)));
        internalCluster().startNode(onlyRoles(Settings.EMPTY, Set.of(DiscoveryNodeRole.DATA_ROLE)));

        updateClusterSettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), true));

        // Phase 1: INGEST consumer only — databases on ingest nodes, not on data-only
        IpLocationTestHelper.requestDownloads(internalCluster(), projectId, IpLocationConsumer.INGEST);
        assertBusy(() -> {
            List<GeoIpStatsAction.NodeResponse> responses = getNodeResponses();
            for (GeoIpStatsAction.NodeResponse nodeResponse : responses) {
                DiscoveryNode node = nodeResponse.getNode();
                if (node.isIngestNode()) {
                    assertThat(
                        "phase 1: ingest node [" + node.getName() + "] should have databases. " + describeCluster(projectId, responses),
                        nodeResponse.getDatabases(),
                        not(empty())
                    );
                } else if (node.canContainData()) {
                    assertThat(
                        "phase 1: data-only node ["
                            + node.getName()
                            + "] should NOT have databases. "
                            + describeCluster(projectId, responses),
                        nodeResponse.getDatabases(),
                        empty()
                    );
                }
            }
        }, PHASE_TIMEOUT.seconds(), TimeUnit.SECONDS);

        // Phase 2: add ESQL consumer — databases should now also appear on data-only nodes
        IpLocationTestHelper.requestDownloads(internalCluster(), projectId, IpLocationConsumer.ESQL);
        IpLocationTestHelper.awaitAllDatabasesAvailable(internalCluster(), IpLocationConsumer.ESQL);

        // Phase 3: cancel INGEST — ESQL still active, data-only nodes keep databases
        IpLocationTestHelper.cancelDownloadRequest(internalCluster(), projectId, IpLocationConsumer.INGEST);
        assertBusy(() -> {
            List<GeoIpStatsAction.NodeResponse> responses = getNodeResponses();
            for (GeoIpStatsAction.NodeResponse nodeResponse : responses) {
                DiscoveryNode node = nodeResponse.getNode();
                if (node.canContainData()) {
                    assertThat(
                        "phase 3: data node ["
                            + node.getName()
                            + "] should still have databases (ESQL active). "
                            + describeCluster(projectId, responses),
                        nodeResponse.getDatabases(),
                        not(empty())
                    );
                }
            }
        }, PHASE_TIMEOUT.seconds(), TimeUnit.SECONDS);

        // Phase 4: cancel ESQL — no consumers left, databases removed from data-only nodes
        IpLocationTestHelper.cancelDownloadRequest(internalCluster(), projectId, IpLocationConsumer.ESQL);
        assertBusy(() -> {
            List<GeoIpStatsAction.NodeResponse> responses = getNodeResponses();
            for (GeoIpStatsAction.NodeResponse nodeResponse : responses) {
                DiscoveryNode node = nodeResponse.getNode();
                if (node.canContainData()) {
                    assertThat(
                        "phase 4: data-only node ["
                            + node.getName()
                            + "] should have no databases after all consumers cancel. "
                            + describeCluster(projectId, responses),
                        nodeResponse.getDatabases(),
                        empty()
                    );
                }
            }
        }, PHASE_TIMEOUT.seconds(), TimeUnit.SECONDS);
    }

    private List<GeoIpStatsAction.NodeResponse> getNodeResponses() {
        GeoIpStatsAction.Response response = client().execute(GeoIpStatsAction.INSTANCE, new GeoIpStatsAction.Request()).actionGet();
        assertThat(response.getNodes(), not(empty()));
        return response.getNodes();
    }

    /**
     * Builds a one-line snapshot of the consumer set + per-node roles + databases for inclusion in
     * assertion failure messages, so a Buildkite log alone is enough to triage.
     */
    private String describeCluster(String projectId, List<GeoIpStatsAction.NodeResponse> responses) {
        IpLocationDownloadConsumers consumers = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .metadata()
            .getProject(ProjectId.fromId(projectId))
            .custom(IpLocationDownloadConsumers.TYPE, IpLocationDownloadConsumers.EMPTY);
        String nodes = responses.stream()
            .map(
                r -> "{name="
                    + r.getNode().getName()
                    + " roles="
                    + r.getNode().getRoles().stream().map(DiscoveryNodeRole::roleName).sorted().collect(Collectors.joining(","))
                    + " dbs="
                    + r.getDatabases()
                    + "}"
            )
            .collect(Collectors.joining(", "));
        return "consumers=" + consumers + " nodes=[" + nodes + "]";
    }

    /**
     * A simple plugin that provides the {@link GeoIpIndexSettingProvider}.
     */
    @SuppressWarnings("NewClassNamingConvention")
    public static final class GeoIpIndexSettingProviderPlugin extends Plugin {
        @Override
        public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
            return List.of(new GeoIpIndexSettingProvider());
        }
    }

    /**
     * Disables the request cache for the {@code .geoip_databases} index so that
     * test assertions against that index always see fresh results.
     */
    @SuppressWarnings("NewClassNamingConvention")
    public static final class GeoIpIndexSettingProvider implements IndexSettingProvider {
        @Override
        public void provideAdditionalSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings,
            IndexVersion indexVersion,
            Settings.Builder additionalSettings
        ) {
            if (GeoIpDownloader.GEOIP_DOWNLOADER.equals(indexName)) {
                additionalSettings.put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), false);
            }
        }
    }
}
