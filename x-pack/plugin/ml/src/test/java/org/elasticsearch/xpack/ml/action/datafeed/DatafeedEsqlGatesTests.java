/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DatafeedEsqlGatesTests extends ESTestCase {

    public void testValidateDatafeedCreateEsqlDatafeedOnUpgradedClusterShouldSucceed() {
        DatafeedConfig datafeed = esqlDatafeed();
        DatafeedEsqlGates.validateDatafeedCreate(datafeed, clusterStateWithMinTransportVersion(TransportVersion.current()));
    }

    public void testValidateDatafeedCreateEsqlDatafeedOnMixedVersionClusterShouldReject() {
        DatafeedConfig datafeed = esqlDatafeed();
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedEsqlGates.validateDatafeedCreate(datafeed, clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()))
        );
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(exception.getMessage(), containsString("cluster upgrade is in progress"));
    }

    public void testValidateDatafeedCreateNonEsqlDatafeedOnMixedVersionClusterShouldSucceed() {
        DatafeedConfig datafeed = new DatafeedConfig.Builder("datafeed-1", "job-1").setIndices(List.of("index-1")).build();
        DatafeedEsqlGates.validateDatafeedCreate(datafeed, clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()));
    }

    public void testValidateDatafeedCreateEsqlDatafeedWhenFlagOffShouldReject() {
        DatafeedConfig datafeed = esqlDatafeed();
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedEsqlGates.validateDatafeedCreate(
                datafeed.getId(),
                datafeed.minRequiredTransportVersion(),
                true,
                clusterStateWithMinTransportVersion(TransportVersion.current()),
                false
            )
        );
        assertThat(exception.getMessage(), equalTo(Messages.getMessage(Messages.DATAFEED_ESQL_CREATE_DISABLED, datafeed.getId())));
    }

    public void testValidateDatafeedUpdateAddEsqlToClassicDatafeedShouldRejectBeforeUpgradeCheck() {
        DatafeedConfig current = new DatafeedConfig.Builder("datafeed-1", "job-1").setIndices(List.of("logs")).build();
        DatafeedUpdate update = new DatafeedUpdate.Builder("datafeed-1").setEsqlQuery("FROM logs").build();
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedEsqlGates.validateDatafeedUpdate(
                current,
                update,
                clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion())
            )
        );
        assertThat(
            exception.getMessage(),
            equalTo(Messages.getMessage(Messages.DATAFEED_ESQL_UPDATE_ADD_QUERY_NOT_ALLOWED, current.getId()))
        );
    }

    public void testValidateDatafeedUpdateTransportOnCoordinatorShouldRejectEsqlFieldsDuringUpgrade() {
        DatafeedUpdate update = new DatafeedUpdate.Builder("datafeed-1").setEsqlQuery("FROM logs").build();
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedEsqlGates.validateDatafeedUpdateTransportOnCoordinator(
                update,
                clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion())
            )
        );
        assertThat(exception.getMessage(), containsString("cluster upgrade is in progress"));
        assertThat(exception.getMessage(), containsString("ES|QL datafeed updates"));
    }

    public void testValidateEsqlDatafeedEnabledWhenFlagOnAndClusterSupportsItShouldSucceed() {
        DatafeedConfig datafeed = esqlDatafeed();
        DatafeedEsqlGates.validateEsqlDatafeedEnabled(
            datafeed,
            clusterStateWithMinTransportVersion(TransportVersion.current()),
            Messages.DATAFEED_ESQL_PREVIEW_UPGRADE_IN_PROGRESS,
            Messages.DATAFEED_ESQL_PREVIEW_DISABLED
        );
    }

    public void testValidateEsqlDatafeedEnabledOnMixedVersionClusterShouldReject() {
        DatafeedConfig datafeed = esqlDatafeed();
        ElasticsearchStatusException exception = expectThrows(
            ElasticsearchStatusException.class,
            () -> DatafeedEsqlGates.validateEsqlDatafeedEnabled(
                datafeed,
                clusterStateWithMinTransportVersion(preEsqlDatafeedTransportVersion()),
                Messages.DATAFEED_ESQL_START_UPGRADE_IN_PROGRESS,
                Messages.DATAFEED_ESQL_START_DISABLED
            )
        );
        assertThat(exception.getMessage(), containsString("cluster upgrade is in progress"));
    }

    private static DatafeedConfig esqlDatafeed() {
        return new DatafeedConfig.Builder("datafeed-1", "job-1").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
    }

    private static ClusterState clusterStateWithMinTransportVersion(TransportVersion transportVersion) {
        return ClusterState.builder(new ClusterName("datafeed-esql-gates-tests"))
            .putCompatibilityVersions("node-1", transportVersion, SystemIndices.SERVER_SYSTEM_MAPPINGS_VERSIONS)
            .build();
    }

    private static TransportVersion preEsqlDatafeedTransportVersion() {
        return TransportVersionUtils.getPreviousVersion(DatafeedConfig.ML_DATAFEED_ESQL_QUERY);
    }
}
