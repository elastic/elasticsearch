/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.cloud.CloudCredentialTestUtils.randomPersistedCloudCredential;
import static org.hamcrest.Matchers.equalTo;

public class EsqlProbePlanTests extends ESTestCase {

    private static final String ESQL_QUERY = "FROM logs-* | STATS c = COUNT(*) BY time = BUCKET(@timestamp, 1h)";
    private static final EsqlProbePlan NONE = new EsqlProbePlan(false, false);
    private static final EsqlProbePlan COLUMNS_ONLY = new EsqlProbePlan(true, false);

    private static Job job(String summaryCountField) {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(new Detector.Builder("count", null).build())
        );
        analysisConfig.setSummaryCountFieldName(summaryCountField);
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));
        return new Job.Builder("job").setAnalysisConfig(analysisConfig).setDataDescription(new DataDescription.Builder()).build(new Date());
    }

    private static DatafeedConfig.Builder esqlDatafeed() {
        return new DatafeedConfig.Builder("df", "job").setEsqlQuery(ESQL_QUERY)
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .setDelayedDataCheckConfig(DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(1)));
    }

    public void testPutPlanShouldRunBothProbes() {
        assertThat(EsqlProbePlan.ALL, equalTo(new EsqlProbePlan(true, true)));
    }

    public void testOnlyCheckWindowChangedShouldRunNoProbes() {
        DatafeedConfig current = esqlDatafeed().build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setDelayedDataCheckConfig(
            DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(2))
        ).build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(NONE));
    }

    public void testDelayedDataCheckEnabledWithJobSummaryFieldShouldRunColumnsProbeOnly() {
        DatafeedConfig current = esqlDatafeed().setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig()).build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setDelayedDataCheckConfig(
            DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(1))
        ).build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(COLUMNS_ONLY));
    }

    public void testDelayedDataCheckDisabledShouldRunNoProbes() {
        DatafeedConfig current = esqlDatafeed().build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setDelayedDataCheckConfig(
            DelayedDataCheckConfig.disabledDelayedDataCheckConfig()
        ).build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(NONE));
    }

    public void testDelayedDataCheckEnabledWithoutJobSummaryFieldShouldRunNoProbes() {
        DatafeedConfig current = esqlDatafeed().setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig()).build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setDelayedDataCheckConfig(
            DelayedDataCheckConfig.enabledDelayedDataCheckConfig(TimeValue.timeValueHours(1))
        ).build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job(null)), equalTo(NONE));
    }

    public void testProjectRoutingChangedShouldRunBothProbes() {
        DatafeedConfig current = esqlDatafeed().setProjectRouting("_alias:_origin").build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setProjectRouting("_alias:prod-*").build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(EsqlProbePlan.ALL));
    }

    public void testCredentialReplacedWithSameRoutingShouldRunBothProbes() {
        DatafeedConfig current = esqlDatafeed().setProjectRouting("_alias:_origin")
            .setCloudInternalCredential(randomPersistedCloudCredential("old-key"))
            .build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setCloudInternalCredential(randomPersistedCloudCredential("new-key"))
            .build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(EsqlProbePlan.ALL));
    }

    public void testCredentialMintedFirstTimeShouldRunBothProbes() {
        DatafeedConfig current = esqlDatafeed().build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setCloudInternalCredential(randomPersistedCloudCredential("new-key"))
            .build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(EsqlProbePlan.ALL));
    }

    public void testCredentialClearedShouldRunBothProbes() {
        DatafeedConfig current = esqlDatafeed().setCloudInternalCredential(randomPersistedCloudCredential("old-key")).build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setCloudInternalCredential(null).build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(EsqlProbePlan.ALL));
    }

    public void testHeadersChangedWithSameCredentialShouldRunNoProbes() {
        DatafeedConfig current = esqlDatafeed().setHeaders(Map.of("es-security-runas-user", "a")).build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setHeaders(Map.of("es-security-runas-user", "b")).build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(NONE));
    }

    public void testOnlyQueryDelayChangedShouldRunNoProbes() {
        DatafeedConfig current = esqlDatafeed().build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setQueryDelay(TimeValue.timeValueSeconds(90)).build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(NONE));
    }

    public void testDslDatafeedShouldRunNoProbes() {
        DatafeedConfig current = new DatafeedConfig.Builder("df", "job").setIndices(List.of("logs-*")).build();
        DatafeedConfig updated = new DatafeedConfig.Builder(current).setProjectRouting("_alias:prod-*").build();
        assertThat(EsqlProbePlan.forUpdate(current, updated, job("c")), equalTo(NONE));
    }
}
