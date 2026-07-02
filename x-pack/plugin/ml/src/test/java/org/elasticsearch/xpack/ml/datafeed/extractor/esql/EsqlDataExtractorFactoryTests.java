/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.junit.Before;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class EsqlDataExtractorFactoryTests extends ESTestCase {

    private static final String ESQL_QUERY = "FROM logs";
    private static final String TIME_FIELD = "ts";
    private static final String SUMMARY_COUNT_FIELD = "doc_count";

    private Client client;
    private DatafeedTimingStatsReporter timingStatsReporter;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
    }

    public void testNewExtractorMapsContextFields() {
        Job job = buildJob("job-1", TIME_FIELD, null);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", null, null);
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(1000L, 2000L);

        assertThat(extractor, notNullValue());
        assertThat(extractor.getEndTime(), equalTo(2000L));
        EsqlDataExtractorContext context = extractor.getContext();
        assertThat(context.jobId(), equalTo("job-1"));
        assertThat(context.esqlQuery(), equalTo(ESQL_QUERY));
        assertThat(context.timeField(), equalTo(TIME_FIELD));
        assertThat(context.start(), equalTo(1000L));
        assertThat(context.end(), equalTo(2000L));
        assertThat(context.requiredSummaryCountField(), nullValue());
    }

    public void testNewExtractorSetsRequiredSummaryCountFieldWhenDelayedDataCheckEnabled() {
        Job job = buildJob("job-1", TIME_FIELD, SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig(), null);
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(0L, 1000L);

        assertThat(extractor.getContext().requiredSummaryCountField(), equalTo(SUMMARY_COUNT_FIELD));
    }

    public void testNewExtractorRequiredSummaryCountFieldNullWhenDelayedDataCheckDisabled() {
        Job job = buildJob("job-1", TIME_FIELD, SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.disabledDelayedDataCheckConfig(), null);
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(0L, 1000L);

        assertThat(extractor.getContext().requiredSummaryCountField(), nullValue());
    }

    public void testNewExtractorPassesHeaders() {
        Job job = buildJob("job-1", TIME_FIELD, null);
        Map<String, String> headers = Map.of("es-security-runas-user", "test-user");
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", null, headers);
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(0L, 1000L);

        assertThat(extractor.getContext().headers(), equalTo(headers));
    }

    public void testCreateInvokesListenerWithEsqlFactory() {
        Job job = buildJob("job-1", TIME_FIELD, null);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", null, null);

        AtomicReference<DataExtractorFactory> onResponse = new AtomicReference<>();
        AtomicBoolean onFailureCalled = new AtomicBoolean(false);
        ActionListener<DataExtractorFactory> listener = ActionListener.wrap(onResponse::set, e -> onFailureCalled.set(true));

        EsqlDataExtractorFactory.create(client, datafeed, job, timingStatsReporter, listener);

        assertThat(onFailureCalled.get(), is(false));
        assertThat(onResponse.get(), instanceOf(EsqlDataExtractorFactory.class));
        EsqlDataExtractorFactory factory = (EsqlDataExtractorFactory) onResponse.get();
        assertThat(factory.client(), equalTo(client));
        assertThat(factory.datafeed(), equalTo(datafeed));
        assertThat(factory.job(), equalTo(job));
        assertThat(factory.timingStatsReporter(), equalTo(timingStatsReporter));
    }

    private static Job buildJob(String jobId, String timeField, String summaryCountFieldName) {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueSeconds(60));
        if (summaryCountFieldName != null) {
            analysisConfig.setSummaryCountFieldName(summaryCountFieldName);
        }
        Job.Builder builder = new Job.Builder(jobId);
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(new DataDescription.Builder().setTimeField(timeField));
        return builder.build(new Date());
    }

    private static DatafeedConfig buildDatafeed(
        String datafeedId,
        String jobId,
        DelayedDataCheckConfig delayedDataCheckConfig,
        Map<String, String> headers
    ) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedId, jobId);
        builder.setEsqlQuery(ESQL_QUERY);
        builder.setDelayedDataCheckConfig(delayedDataCheckConfig);
        if (headers != null) {
            builder.setHeaders(headers);
        }
        return builder.build();
    }
}
