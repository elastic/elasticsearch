/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
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

import static org.elasticsearch.test.NodeRoles.onlyRoles;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * The PUT probe filters {@code DESCRIBE} on the time column and the summary count column. With two different names the filter
 * is evaluated as a keyword {@code IN}, which needs the {@code mapper-version} classes on the plugin classpath. The node churn IT
 * has no summary count column, so it cannot see this case.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EsqlDatafeedSummaryCountValidationIT extends BaseMlIntegTestCase {

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

    public void testPutDatafeedWithDistinctSummaryCountColumnShouldPass() throws Exception {
        internalCluster().ensureAtMostNumDataNodes(0);
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode(onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.ML_ROLE)));
        ensureStableCluster(2);

        String indexName = "esql-summary-count-data";
        assertAcked(indicesAdmin().prepareCreate(indexName).setMapping("@timestamp", "type=date"));

        String jobId = "esql-summary-count-job";
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        analysisConfig.setSummaryCountFieldName("doc_count");
        Job.Builder job = new Job.Builder(jobId);
        job.setAnalysisConfig(analysisConfig);
        job.setDataDescription(new DataDescription.Builder().setTimeField("@timestamp").setTimeFormat(DataDescription.EPOCH_MS));
        client().execute(PutJobAction.INSTANCE, new PutJobAction.Request(job)).actionGet();

        // The delayed data check is enabled by default, so the probe requires both "@timestamp" and "doc_count".
        DatafeedConfig datafeed = new DatafeedConfig.Builder(jobId + "-datafeed", jobId).setEsqlQuery(
            "FROM " + indexName + " | STATS doc_count = COUNT(*) BY @timestamp = BUCKET(@timestamp, 1 hour)"
        ).setSourceTimeField("@timestamp").setGroupingInterval(TimeValue.timeValueHours(1)).build();

        PutDatafeedAction.Response response = client().execute(PutDatafeedAction.INSTANCE, new PutDatafeedAction.Request(datafeed))
            .actionGet();
        assertThat(response.getResponse().getId(), equalTo(jobId + "-datafeed"));
    }
}
