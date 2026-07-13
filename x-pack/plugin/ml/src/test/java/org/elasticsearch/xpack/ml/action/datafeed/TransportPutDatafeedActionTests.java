/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.MachineLearningExtension;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

public class TransportPutDatafeedActionTests extends ESTestCase {

    public void testCheckClusterSupportsDatafeedConfig_UpgradedCluster_EsqlDatafeed() {
        DatafeedConfig datafeed = esqlDatafeed();
        ClusterState clusterState = clusterStateWithMinTransportVersion(TransportVersion.current());
        assertTrue(TransportPutDatafeedAction.checkClusterSupportsDatafeedConfig(datafeed, clusterState).isEmpty());
    }

    public void testCheckClusterSupportsDatafeedConfig_MixedVersionCluster_EsqlDatafeed() {
        DatafeedConfig datafeed = esqlDatafeed();
        ClusterState clusterState = clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion());
        assertTrue(TransportPutDatafeedAction.checkClusterSupportsDatafeedConfig(datafeed, clusterState).isPresent());
    }

    public void testCheckClusterSupportsDatafeedConfig_MixedVersionCluster_NonEsqlDatafeed() {
        DatafeedConfig datafeed = new DatafeedConfig.Builder("datafeed-1", "job-1").setIndices(List.of("index-1")).build();
        ClusterState clusterState = clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion());

        assertTrue(TransportPutDatafeedAction.checkClusterSupportsDatafeedConfig(datafeed, clusterState).isEmpty());
    }

    public void testMasterOperation_MixedVersionCluster_EsqlDatafeedRejected() {
        DatafeedConfig datafeedConfig = esqlDatafeed();
        DatafeedConfigProvider datafeedConfigProvider = mock(DatafeedConfigProvider.class);
        JobConfigProvider jobConfigProvider = mock(JobConfigProvider.class);
        Client client = mock(Client.class);
        TransportPutDatafeedAction action = createAction(datafeedConfigProvider, jobConfigProvider, client);
        ClusterState clusterState = clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion());

        try (PutDatafeedAction.Request request = new PutDatafeedAction.Request(datafeedConfig)) {
            AtomicReference<Exception> failure = new AtomicReference<>();
            action.masterOperation(null, request, clusterState, ActionTestUtils.assertNoSuccessListener(failure::set));

            assertThat(failure.get(), instanceOf(ElasticsearchStatusException.class));
            assertThat(((ElasticsearchStatusException) failure.get()).status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(
                failure.get().getMessage(),
                equalTo(
                    "Cannot create datafeed [datafeed-1] while a cluster upgrade is in progress "
                        + "(datafeed uses an ES|QL query, which requires support for ES|QL datafeeds); "
                        + "wait for the cluster to finish upgrading and try again."
                )
            );
            verifyNoInteractions(datafeedConfigProvider, jobConfigProvider, client);
        }
    }

    private static DatafeedConfig esqlDatafeed() {
        return new DatafeedConfig.Builder("datafeed-1", "job-1").setEsqlQuery("FROM logs").build();
    }

    private static TransportPutDatafeedAction createAction(
        DatafeedConfigProvider datafeedConfigProvider,
        JobConfigProvider jobConfigProvider,
        Client client
    ) {
        Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        DatafeedManager datafeedManager = new DatafeedManager(
            datafeedConfigProvider,
            jobConfigProvider,
            NamedXContentRegistry.EMPTY,
            settings,
            client,
            mock(MachineLearningExtension.class),
            mock(AnomalyDetectionAuditor.class)
        );
        return new TransportPutDatafeedAction(
            settings,
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(XPackLicenseState.class),
            mock(ActionFilters.class),
            datafeedManager,
            mock(ProjectResolver.class)
        );
    }

    private static ClusterState clusterStateWithMinTransportVersion(TransportVersion transportVersion) {
        return ClusterState.builder(new ClusterName("put-datafeed-action-tests"))
            .putCompatibilityVersions("node-1", transportVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private static TransportVersion preEsqlDatafeedTransportVersion() {
        return TransportVersion.fromName("histogram_blocks_multivalue_support");
    }
}
