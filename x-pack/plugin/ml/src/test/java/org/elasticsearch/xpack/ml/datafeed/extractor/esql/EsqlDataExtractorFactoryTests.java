/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.ExecutionMetadata;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlDataExtractorFactoryTests extends ESTestCase {

    private static final String ESQL_QUERY =
        "FROM logs | STATS avg_bytes = AVG(bytes), doc_count = COUNT(*) BY ts = BUCKET(@timestamp, 1 hour)";
    private static final String TIME_FIELD = "ts";
    private static final String SOURCE_TIME_FIELD = "@timestamp";
    private static final String SUMMARY_COUNT_FIELD = "doc_count";

    private Client client;
    private DatafeedTimingStatsReporter timingStatsReporter;

    @Before
    public void setUpMocks() {
        client = mock(Client.class);
        timingStatsReporter = mock(DatafeedTimingStatsReporter.class);
    }

    public void testNewExtractorShouldMapContextFields() {
        Job job = buildJob("job-1", TIME_FIELD, null);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", null, null);
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter, 0L);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(1000L, 2000L);

        assertThat(extractor, notNullValue());
        assertThat(extractor.getEndTime(), equalTo(2000L));
        EsqlDataExtractorContext context = extractor.getContext();
        assertThat(context.jobId(), equalTo("job-1"));
        assertThat(context.esqlQuery(), equalTo(ESQL_QUERY));
        assertThat(context.sourceTimeField(), equalTo(SOURCE_TIME_FIELD));
        assertThat(context.emittedTimeField(), equalTo(TIME_FIELD));
        assertThat(context.sourceTimeField(), not(equalTo(context.emittedTimeField())));
        assertThat(context.start(), equalTo(1000L));
        assertThat(context.end(), equalTo(2000L));
        assertThat(context.requiredSummaryCountField(), nullValue());
        assertThat(context.projectRouting(), nullValue());
    }

    public void testDelayedDataCheckEnabledShouldSetRequiredSummaryCountFieldOnNewExtractor() {
        Job job = buildJob("job-1", TIME_FIELD, SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig(), null);
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter, 0L);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(0L, 1000L);

        assertThat(extractor.getContext().requiredSummaryCountField(), equalTo(SUMMARY_COUNT_FIELD));
    }

    public void testNewExtractorShouldPassHeaders() {
        Job job = buildJob("job-1", TIME_FIELD, null);
        Map<String, String> headers = Map.of("es-security-runas-user", "test-user");
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", null, headers);
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter, 0L);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(0L, 1000L);

        assertThat(extractor.getContext().headers(), equalTo(headers));
    }

    public void testProjectRoutingShouldBeCopiedIntoContextOnNewExtractor() {
        Job job = buildJob("job-1", TIME_FIELD, null);
        DatafeedConfig datafeed = buildDatafeedWithProjectRouting("datafeed-1", "job-1", "_alias:_origin");
        EsqlDataExtractorFactory factory = new EsqlDataExtractorFactory(client, datafeed, job, timingStatsReporter, 0L);

        EsqlDataExtractor extractor = (EsqlDataExtractor) factory.newExtractor(0L, 1000L);

        assertThat(extractor.getContext().projectRouting(), equalTo("_alias:_origin"));
    }

    public void testCreateShouldRunOneDescribeAndFlagRawQueryAsNotBucketed() {
        StubRunner runner = new StubRunner(describeResponse(row(TIME_FIELD, "date", "time", SOURCE_TIME_FIELD, "mapped", null)));

        EsqlDataExtractorFactory factory = createFactory(runner);

        assertThat(factory.timeBucketMillis(), equalTo(0L));
        assertThat(
            runner.queries,
            equalTo(List.of(ESQL_QUERY + "\n| DESCRIBE\n| WHERE name == ?describeTimeColumn OR name == ?describeCountColumn\n| LIMIT 2"))
        );
        assertThat(runner.params, equalTo(describeParams(TIME_FIELD, TIME_FIELD)));
    }

    public void testCreateShouldTakeBucketLengthFromTheIntervalOfTheTimeColumn() {
        assertThat(createFactoryWithInterval("1 hour").timeBucketMillis(), equalTo(TimeValue.timeValueHours(1).millis()));
        assertThat(createFactoryWithInterval("90 minutes").timeBucketMillis(), equalTo(TimeValue.timeValueMinutes(90).millis()));
        assertThat(createFactoryWithInterval("1 day").timeBucketMillis(), equalTo(TimeValue.timeValueDays(1).millis()));
    }

    public void testCreateShouldFailWhenTimeColumnIsBucketedByCalendarInterval() {
        StubRunner runner = new StubRunner(describeResponse(row(TIME_FIELD, "date", "time", null, "computed", "1 month")));

        Exception e = createFactoryFailure(runner);

        assertThat(e, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) e).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("1 month"));
    }

    public void testCreateShouldRunDescribeWithDatafeedHeadersAndProjectRouting() {
        StubRunner runner = new StubRunner(describeResponse(row(TIME_FIELD, "date", "time", SOURCE_TIME_FIELD, "mapped", null)));
        Map<String, String> headers = Map.of("es-security-runas-user", "test-user");
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("datafeed-1", "job-1");
        builder.setEsqlQuery(ESQL_QUERY);
        builder.setSourceTimeField(SOURCE_TIME_FIELD);
        builder.setGroupingInterval(TimeValue.timeValueMinutes(1));
        builder.setHeaders(headers);
        builder.setProjectRouting("_alias:_origin");

        createFactory(runner, builder.build());

        assertThat(runner.headers, equalTo(headers));
        assertThat(runner.projectRouting, equalTo("_alias:_origin"));
    }

    public void testCreateShouldPropagateDescribeFailure() {
        for (Exception failure : List.<Exception>of(
            new IndexNotFoundException("logs"),
            new ElasticsearchStatusException("forbidden", RestStatus.FORBIDDEN)
        )) {
            assertThat(createFactoryFailure(new StubRunner(failure)), sameInstance(failure));
        }
    }

    public void testCreateShouldDescribeTheSummaryCountColumnWhenDelayedDataCheckNeedsIt() {
        Job job = buildJob("job-1", TIME_FIELD, SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig(), null);
        StubRunner runner = new StubRunner(
            describeResponse(
                row(TIME_FIELD, "date", "time", null, "computed", null),
                row(SUMMARY_COUNT_FIELD, "long", "count", null, "computed", null)
            )
        );

        createFactory(runner, datafeed, job);

        assertThat(runner.params, equalTo(describeParams(TIME_FIELD, SUMMARY_COUNT_FIELD)));
    }

    public void testCreateShouldDescribeOnlyTheTimeColumnWhenNoSummaryCountIsRequired() {
        StubRunner runner = new StubRunner(describeResponse(row(TIME_FIELD, "date", "time", null, "computed", null)));

        createFactory(runner);

        assertThat(runner.params, equalTo(describeParams(TIME_FIELD, TIME_FIELD)));
    }

    public void testCreateShouldFailWhenTimeColumnIsNotDate() {
        Exception failure = createFactoryFailure(
            new StubRunner(describeResponse(row(TIME_FIELD, "keyword", null, null, "computed", null)))
        );

        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("[" + TIME_FIELD + "]"));
        assertThat(failure.getMessage(), containsString("keyword"));
    }

    public void testCreateShouldFailWhenRequiredSummaryCountColumnIsMissing() {
        Job job = buildJob("job-1", TIME_FIELD, SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig(), null);
        StubRunner runner = new StubRunner(describeResponse(row(TIME_FIELD, "date", "time", null, "computed", null)));

        Exception failure = createFactoryFailure(runner, datafeed, job);

        assertThat(failure.getMessage(), containsString(SUMMARY_COUNT_FIELD));
    }

    public void testCreateShouldFailWhenOnlyRemoteWasSkippedWithoutDescribeRows() {
        EsqlQueryResponse response = describeResponse(List.of(new EsqlClusterInfo("east", "logs-*", EsqlClusterInfo.Status.SKIPPED)));

        Exception failure = createFactoryFailure(new StubRunner(response));

        assertThat(failure, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.getMessage(), containsString("[east]"));
        assertThat(failure.getMessage(), containsString("[" + TIME_FIELD + "]"));
    }

    private EsqlDataExtractorFactory createFactoryWithInterval(String interval) {
        return createFactory(new StubRunner(describeResponse(row(TIME_FIELD, "date", "time", null, "computed", interval))));
    }

    private EsqlDataExtractorFactory createFactory(StubRunner runner) {
        return createFactory(runner, buildDatafeed("datafeed-1", "job-1", null, null));
    }

    private EsqlDataExtractorFactory createFactory(StubRunner runner, DatafeedConfig datafeed) {
        return createFactory(runner, datafeed, buildJob("job-1", TIME_FIELD, null));
    }

    private EsqlDataExtractorFactory createFactory(StubRunner runner, DatafeedConfig datafeed, Job job) {
        AtomicReference<DataExtractorFactory> onResponse = new AtomicReference<>();
        AtomicReference<Exception> onFailure = new AtomicReference<>();
        EsqlDataExtractorFactory.create(
            runner,
            client,
            datafeed,
            job,
            timingStatsReporter,
            ActionListener.wrap(onResponse::set, onFailure::set)
        );
        assertThat(onFailure.get(), nullValue());
        assertThat(onResponse.get(), instanceOf(EsqlDataExtractorFactory.class));
        EsqlDataExtractorFactory factory = (EsqlDataExtractorFactory) onResponse.get();
        assertThat(factory.client(), equalTo(client));
        assertThat(factory.datafeed(), equalTo(datafeed));
        assertThat(factory.job(), equalTo(job));
        assertThat(factory.timingStatsReporter(), equalTo(timingStatsReporter));
        return factory;
    }

    private Exception createFactoryFailure(StubRunner runner) {
        return createFactoryFailure(runner, buildDatafeed("datafeed-1", "job-1", null, null), buildJob("job-1", TIME_FIELD, null));
    }

    private Exception createFactoryFailure(StubRunner runner, DatafeedConfig datafeed, Job job) {
        AtomicReference<DataExtractorFactory> onResponse = new AtomicReference<>();
        AtomicReference<Exception> onFailure = new AtomicReference<>();
        EsqlDataExtractorFactory.create(
            runner,
            client,
            datafeed,
            job,
            timingStatsReporter,
            ActionListener.wrap(onResponse::set, onFailure::set)
        );
        assertThat(onResponse.get(), nullValue());
        assertThat(onFailure.get(), notNullValue());
        return onFailure.get();
    }

    private static List<EsqlQueryParam> describeParams(String timeField, String countField) {
        return List.of(
            new EsqlQueryParam("describeTimeColumn", timeField, EsqlQueryParam.ParamClassification.VALUE),
            new EsqlQueryParam("describeCountColumn", countField, EsqlQueryParam.ParamClassification.VALUE)
        );
    }

    private static final List<String> DESCRIBE_COLUMNS = List.of("name", "type", "role", "field", "origin", "interval");

    private static List<Object> row(String name, String type, String role, String field, String origin, String interval) {
        return Arrays.asList(name, type, role, field, origin, interval);
    }

    /** A response without rows and without DESCRIBE columns, as ES|QL returns it when every source was skipped. */
    private static EsqlQueryResponse describeResponse(List<EsqlClusterInfo> clusters) {
        EsqlResponse response = mock(EsqlResponse.class);
        doReturn(List.of()).when(response).columns();
        when(response.rows()).thenReturn(List.of());
        doReturn(clusters).when(response).clusters();
        EsqlQueryResponse queryResponse = mock(EsqlQueryResponse.class);
        when(queryResponse.response()).thenReturn(response);
        return queryResponse;
    }

    @SafeVarargs
    @SuppressWarnings({ "unchecked", "varargs" })
    private static EsqlQueryResponse describeResponse(List<Object>... rows) {
        EsqlResponse response = mock(EsqlResponse.class);
        List<ColumnInfo> header = new ArrayList<>();
        for (String name : DESCRIBE_COLUMNS) {
            ColumnInfo column = mock(ColumnInfo.class);
            when(column.name()).thenReturn(name);
            header.add(column);
        }
        doReturn(header).when(response).columns();
        when(response.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) Arrays.asList(rows));
        EsqlQueryResponse queryResponse = mock(EsqlQueryResponse.class);
        when(queryResponse.response()).thenReturn(response);
        return queryResponse;
    }

    /** Answers every query with a canned DESCRIBE response or failure and records what was asked. */
    private static final class StubRunner implements EsqlQueryRunner {
        private final EsqlQueryResponse response;
        private final Exception failure;
        final List<String> queries = new ArrayList<>();
        Map<String, String> headers;
        String projectRouting;
        List<EsqlQueryParam> params;

        StubRunner(EsqlQueryResponse response) {
            this.response = response;
            this.failure = null;
        }

        StubRunner(Exception failure) {
            this.response = null;
            this.failure = failure;
        }

        @Override
        public void executeAsync(
            Client client,
            String query,
            QueryBuilder filter,
            Map<String, String> headers,
            String projectRouting,
            List<EsqlQueryParam> params,
            ExecutionMetadata executionMetadata,
            ActionListener<EsqlQueryResponse> listener
        ) {
            queries.add(query);
            this.headers = headers;
            this.projectRouting = projectRouting;
            this.params = params;
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(response);
            }
        }
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
        builder.setSourceTimeField(SOURCE_TIME_FIELD);
        builder.setGroupingInterval(TimeValue.timeValueMinutes(1));
        builder.setDelayedDataCheckConfig(delayedDataCheckConfig);
        if (headers != null) {
            builder.setHeaders(headers);
        }
        return builder.build();
    }

    private static DatafeedConfig buildDatafeedWithProjectRouting(String datafeedId, String jobId, String projectRouting) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedId, jobId);
        builder.setEsqlQuery(ESQL_QUERY);
        builder.setSourceTimeField(SOURCE_TIME_FIELD);
        builder.setGroupingInterval(TimeValue.timeValueMinutes(1));
        builder.setProjectRouting(projectRouting);
        return builder.build();
    }
}
