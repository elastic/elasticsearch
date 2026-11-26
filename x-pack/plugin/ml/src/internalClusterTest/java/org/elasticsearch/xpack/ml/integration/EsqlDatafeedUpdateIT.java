/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Proves the {@code _update} semantics of ES|QL datafeeds through the real transport and manager path: any update of a started
 * datafeed conflicts, and once stopped only operational settings (or an unchanged {@code esql_query}) are accepted.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlDatafeedUpdateIT extends BaseMlIntegTestCase {

    private static final String JOB_ID = "esql-update-job";
    private static final String DATAFEED_ID = JOB_ID + "-datafeed";
    private static final String INDEX_NAME = "esql-update-data";
    private static final String ESQL_QUERY = "FROM " + INDEX_NAME + " | KEEP @timestamp";

    @BeforeClass
    public static void checkFeatureFlagEnabled() {
        assumeTrue("ES|QL datafeeds feature flag must be enabled", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put("esql.federation.enabled", false).build();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            EsqlCorePlugin.class,
            EncryptionPlugin.class,
            EsqlPlugin.class
        );
    }

    public void testUpdateOfStartedDatafeedShouldConflictAndSucceedAfterStop() throws Exception {
        putJobAndDatafeed();
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(JOB_ID)).actionGet();
        long start = System.currentTimeMillis() - TimeValue.timeValueHours(3).millis();
        client().execute(StartDatafeedAction.INSTANCE, new StartDatafeedAction.Request(DATAFEED_ID, start)).actionGet();
        assertBusy(() -> assertThat(getDatafeedState(DATAFEED_ID), equalTo(DatafeedState.STARTED)), 30, TimeUnit.SECONDS);

        for (DatafeedUpdate update : new DatafeedUpdate[] {
            new DatafeedUpdate.Builder(DATAFEED_ID).setQueryDelay(TimeValue.timeValueMinutes(3)).build(),
            new DatafeedUpdate.Builder(DATAFEED_ID).setEsqlQuery(ESQL_QUERY).build(),
            new DatafeedUpdate.Builder(DATAFEED_ID).setEsqlQuery("FROM " + INDEX_NAME + " | KEEP @timestamp, host").build() }) {
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> client().execute(UpdateDatafeedAction.INSTANCE, new UpdateDatafeedAction.Request(update)).actionGet()
            );
            assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        }

        client().execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(DATAFEED_ID)).actionGet();
        assertBusy(() -> assertThat(getDatafeedState(DATAFEED_ID), equalTo(DatafeedState.STOPPED)), 30, TimeUnit.SECONDS);

        DatafeedUpdate update = new DatafeedUpdate.Builder(DATAFEED_ID).setQueryDelay(TimeValue.timeValueMinutes(3)).build();
        PutDatafeedAction.Response response = client().execute(UpdateDatafeedAction.INSTANCE, new UpdateDatafeedAction.Request(update))
            .actionGet();
        assertThat(response.getResponse().getQueryDelay(), equalTo(TimeValue.timeValueMinutes(3)));
        assertThat(response.getResponse().getEsqlQuery(), equalTo(ESQL_QUERY));
    }

    public void testUpdateOfStoppedDatafeedShouldAcceptUnchangedEsqlQueryAndRejectChangedOne() throws Exception {
        putJobAndDatafeed();

        DatafeedUpdate identical = new DatafeedUpdate.Builder(DATAFEED_ID).setEsqlQuery(ESQL_QUERY).build();
        PutDatafeedAction.Response response = client().execute(UpdateDatafeedAction.INSTANCE, new UpdateDatafeedAction.Request(identical))
            .actionGet();
        assertThat(response.getResponse().getEsqlQuery(), equalTo(ESQL_QUERY));

        DatafeedUpdate changed = new DatafeedUpdate.Builder(DATAFEED_ID).setEsqlQuery("FROM " + INDEX_NAME + " | KEEP @timestamp, host")
            .build();
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> client().execute(UpdateDatafeedAction.INSTANCE, new UpdateDatafeedAction.Request(changed)).actionGet()
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("Recreate ES|QL datafeed [" + DATAFEED_ID + "]"));
    }

    private void putJobAndDatafeed() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster(2);

        assertAcked(indicesAdmin().prepareCreate(INDEX_NAME).setMapping("@timestamp", "type=date"));
        long now = System.currentTimeMillis();
        client().prepareIndex(INDEX_NAME)
            .setSource("@timestamp", now - TimeValue.timeValueHours(2).millis())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        Job.Builder job = new Job.Builder(JOB_ID);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(new DataDescription.Builder().setTimeField("@timestamp").setTimeFormat(DataDescription.EPOCH_MS));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        DatafeedConfig datafeed = new DatafeedConfig.Builder(DATAFEED_ID, JOB_ID).setEsqlQuery(ESQL_QUERY)
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig())
            .build();
        client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(datafeed)).actionGet();
    }
}
