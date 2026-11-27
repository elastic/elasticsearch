/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.encryption.EncryptionPlugin;
import org.elasticsearch.xpack.esql.core.plugin.EsqlCorePlugin;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractorFactory;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.BeforeClass;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Runs the real ES|QL {@code | DESCRIBE} through {@link EsqlDataExtractorFactory#create} and checks how the datafeed reads the interval
 * and time zone of the time column. The unit tests feed literal interval strings, so only this test notices when ES|QL changes how it
 * writes them.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlDatafeedDescribeIntervalIT extends BaseMlIntegTestCase {

    private static final String INDEX = "esql-describe-interval-data";

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

    public void testRawQueryShouldNotBeBucketed() throws Exception {
        setUpCluster();

        assertThat(timeBucketMillis("FROM " + INDEX + " | KEEP @timestamp"), equalTo(0L));
    }

    public void testHourBucketShouldBeOneHour() throws Exception {
        setUpCluster();

        assertThat(
            timeBucketMillis("FROM " + INDEX + " | STATS c = COUNT(*) BY @timestamp = BUCKET(@timestamp, 1 hour)"),
            equalTo(TimeValue.timeValueHours(1).millis())
        );
    }

    public void testTwentyFourHourBucketShouldBeOneDay() throws Exception {
        setUpCluster();

        assertThat(
            timeBucketMillis("FROM " + INDEX + " | STATS c = COUNT(*) BY @timestamp = BUCKET(@timestamp, 24 hours)"),
            equalTo(TimeValue.timeValueDays(1).millis())
        );
    }

    public void testCalendarMonthBucketShouldBeRejected() throws Exception {
        setUpCluster();

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> timeBucketMillis("FROM " + INDEX + " | STATS c = COUNT(*) BY @timestamp = DATE_TRUNC(1 month, @timestamp)")
        );

        assertThat(e.getMessage(), containsString("calendar interval"));
    }

    public void testBucketInATimeZoneOtherThanUtcShouldBeRejected() throws Exception {
        setUpCluster();

        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> timeBucketMillis(
                "SET time_zone=\"Europe/Berlin\"; FROM " + INDEX + " | STATS c = COUNT(*) BY @timestamp = BUCKET(@timestamp, 1 day)"
            )
        );

        assertThat(e.getMessage(), containsString("Europe/Berlin"));
    }

    private void setUpCluster() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster(2);
        client().admin().indices().prepareCreate(INDEX).setSettings(indexSettings(1, 0)).setMapping("@timestamp", "type=date").get();
        ensureGreen(INDEX);
    }

    private long timeBucketMillis(String esqlQuery) {
        Job.Builder jobBuilder = new Job.Builder("esql-describe-interval-job");
        jobBuilder.setAnalysisConfig(
            new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("count", null).build())).setBucketSpan(
                TimeValue.timeValueHours(1)
            )
        );
        jobBuilder.setDataDescription(new DataDescription.Builder().setTimeField("@timestamp").setTimeFormat(DataDescription.EPOCH_MS));
        Job job = jobBuilder.build(new Date());
        DatafeedConfig datafeed = new DatafeedConfig.Builder("esql-describe-interval-datafeed", job.getId()).setEsqlQuery(esqlQuery)
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig())
            .build();

        PlainActionFuture<DataExtractorFactory> factory = new PlainActionFuture<>();
        EsqlDataExtractorFactory.create(
            client(),
            datafeed,
            job,
            new DatafeedTimingStatsReporter(new DatafeedTimingStats(job.getId()), (stats, refreshPolicy, listener) -> {}),
            factory
        );
        return factory.actionGet().timeBucketMillis();
    }
}
