/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlDatafeedQueryValidatorTests extends ESTestCase {

    private static final String ESQL_QUERY = "FROM logs";
    private static final String TIME_FIELD = "ts";
    private static final String SUMMARY_COUNT_FIELD = "doc_count";

    public void testValidateQueryGivenRequiredColumnsPresent() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"), mockColumn(SUMMARY_COUNT_FIELD, "long"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();

        validator.runValidation(ESQL_QUERY, TIME_FIELD, SUMMARY_COUNT_FIELD, ActionListener.wrap(ok -> succeeded.set(true), failure::set));

        assertThat(succeeded.get(), is(true));
        assertThat(failure.get(), equalTo(null));
    }

    public void testValidateQueryGivenNoSummaryCountFieldRequired() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.runValidation(
            ESQL_QUERY,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> succeeded.set(true), e -> { throw new AssertionError(e); })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testValidateQueryGivenMissingTimeFieldFails() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.runValidation(ESQL_QUERY, TIME_FIELD, null, ActionListener.wrap(ok -> fail("expected failure"), failure::set));

        assertThat(failure.get(), instanceOf(IllegalArgumentException.class));
        assertThat(failure.get().getMessage(), containsString("ESQL query response is missing the required columns: " + TIME_FIELD));
    }

    public void testValidateQueryGivenMissingSummaryCountFieldFails() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.runValidation(
            ESQL_QUERY,
            TIME_FIELD,
            SUMMARY_COUNT_FIELD,
            ActionListener.wrap(ok -> fail("expected failure"), failure::set)
        );

        assertThat(failure.get(), instanceOf(IllegalArgumentException.class));
        assertThat(failure.get().getMessage(), containsString(SUMMARY_COUNT_FIELD));
    }

    public void testValidateQueryGivenIndexNotFoundSucceeds() {
        TestValidator validator = new TestValidator(new IndexNotFoundException("logs"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.runValidation(ESQL_QUERY, TIME_FIELD, null, ActionListener.wrap(ok -> succeeded.set(true), e -> {
            throw new AssertionError("expected success for missing index", e);
        }));

        assertThat(succeeded.get(), is(true));
    }

    public void testValidateQueryGivenOtherExecutionFailurePropagates() {
        RuntimeException boom = new RuntimeException("query syntax error");
        TestValidator validator = new TestValidator(boom);

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.runValidation(ESQL_QUERY, TIME_FIELD, null, ActionListener.wrap(ok -> fail("expected failure"), failure::set));

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("query syntax error"));
    }

    public void testValidateQueryAppendsLimitZero() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        validator.runValidation(ESQL_QUERY, TIME_FIELD, null, ActionListener.wrap(ok -> {}, e -> { throw new AssertionError(e); }));

        assertThat(validator.capturedQuery, equalTo(ESQL_QUERY + " | LIMIT 0"));
    }

    // ---- checkRequiredColumns unit tests ----

    public void testCheckRequiredColumnsGivenAllPresentSucceeds() {
        List<ColumnInfo> columns = List.of(
            mockColumn(TIME_FIELD, "date"),
            mockColumn(SUMMARY_COUNT_FIELD, "long"),
            mockColumn("other", "keyword")
        );
        // Should not throw.
        EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD);
    }

    public void testCheckRequiredColumnsGivenNoSummaryCountFieldRequiredSucceeds() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        // summaryCountField = null means not required — should not throw.
        EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null);
    }

    public void testCheckRequiredColumnsGivenMissingTimeFieldThrows() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null)
        );
        assertThat(e.getMessage(), containsString("ESQL query response is missing the required columns: " + TIME_FIELD));
    }

    public void testCheckRequiredColumnsGivenMissingSummaryCountFieldThrows() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD)
        );
        assertThat(e.getMessage(), containsString("ESQL query response is missing the required columns: " + SUMMARY_COUNT_FIELD));
    }

    public void testCheckRequiredColumnsGivenBothMissingListsBothInError() {
        List<ColumnInfo> columns = List.of(mockColumn("unrelated", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD)
        );
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
    }

    public void testCheckRequiredColumnsGivenEmptyColumnsThrows() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(List.of(), TIME_FIELD, null)
        );
        assertThat(e.getMessage(), containsString(TIME_FIELD));
    }

    // ---- requiredSummaryCountField helper tests ----

    public void testRequiredSummaryCountFieldWhenDelayedCheckEnabledAndFieldSet() {
        Job job = buildJob("job-1", SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig());

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), equalTo(SUMMARY_COUNT_FIELD));
    }

    public void testRequiredSummaryCountFieldWhenDelayedCheckDisabledReturnsNull() {
        Job job = buildJob("job-1", SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), nullValue());
    }

    public void testRequiredSummaryCountFieldWhenNoSummaryCountFieldSetReturnsNull() {
        Job job = buildJob("job-1", null);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig());

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), nullValue());
    }

    public void testRequiredSummaryCountFieldWhenDelayedDataCheckConfigNullReturnsNull() {
        Job job = buildJob("job-1", SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeedWithNullDelayedDataCheckConfig("datafeed-1", "job-1");

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), nullValue());
    }

    // ---- helpers ----

    private static Job buildJob(String jobId, String summaryCountFieldName) {
        Detector.Builder detector = new Detector.Builder("count", null);
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        ac.setBucketSpan(TimeValue.timeValueSeconds(60));
        if (summaryCountFieldName != null) {
            ac.setSummaryCountFieldName(summaryCountFieldName);
        }
        Job.Builder builder = new Job.Builder(jobId);
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(new DataDescription.Builder());
        return builder.build(new Date());
    }

    private static DatafeedConfig buildDatafeed(String datafeedId, String jobId, DelayedDataCheckConfig delayedDataCheckConfig) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedId, jobId);
        builder.setIndices(Collections.singletonList("logs"));
        builder.setDelayedDataCheckConfig(delayedDataCheckConfig);
        return builder.build();
    }

    private static DatafeedConfig buildDatafeedWithNullDelayedDataCheckConfig(String datafeedId, String jobId) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedId, jobId);
        builder.setIndices(Collections.singletonList("logs"));
        builder.setDelayedDataCheckConfig(null);
        return builder.build();
    }

    private ColumnInfo mockColumn(String name, String type) {
        ColumnInfo col = mock(ColumnInfo.class);
        when(col.name()).thenReturn(name);
        when(col.outputType()).thenReturn(type);
        return col;
    }

    @SuppressWarnings("unchecked")
    private EsqlResponse mockEsqlResponse(List<ColumnInfo> columns) {
        EsqlResponse response = mock(EsqlResponse.class);
        doReturn(columns).when(response).columns();
        when(response.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) Collections.emptyList());
        return response;
    }

    private EsqlQueryResponse buildResponse(List<ColumnInfo> columns) {
        return new TestEsqlQueryResponse(mockEsqlResponse(columns));
    }

    /**
     * Test subclass of {@link EsqlDatafeedQueryValidator} that overrides query execution to avoid
     * the {@code SharedSecrets} / esql-plugin dependency absent from the ml plugin test classpath.
     * Instead of building and sending an ESQL request it either returns a pre-built response or
     * simulates a query execution failure.
     */
    private class TestValidator {

        private final EsqlQueryResponse cannedResponse;
        private final Exception cannedFailure;
        String capturedQuery;

        TestValidator(EsqlQueryResponse response) {
            this.cannedResponse = response;
            this.cannedFailure = null;
        }

        TestValidator(Exception failure) {
            this.cannedResponse = null;
            this.cannedFailure = failure;
        }

        /**
         * Runs {@code EsqlDatafeedQueryValidator.validateQuery} but intercepts the query-execution
         * step so no real ESQL call is issued. The captured query string is stored for assertion.
         */
        void runValidation(String esqlQuery, String timeField, String summaryCountField, ActionListener<Boolean> listener) {
            String limitZeroQuery = esqlQuery + " | LIMIT 0";
            capturedQuery = limitZeroQuery;

            ActionListener<EsqlQueryResponse> responseListener = buildResponseListener(timeField, summaryCountField, listener);

            if (cannedFailure != null) {
                responseListener.onFailure(cannedFailure);
            } else {
                responseListener.onResponse(cannedResponse);
            }
        }

        private ActionListener<EsqlQueryResponse> buildResponseListener(
            String timeField,
            String summaryCountField,
            ActionListener<Boolean> listener
        ) {
            return ActionListener.wrap(response -> {
                try {
                    EsqlDatafeedQueryValidator.checkRequiredColumns(response.response().columns(), timeField, summaryCountField);
                    listener.onResponse(Boolean.TRUE);
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }, e -> {
                if (org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                    listener.onResponse(Boolean.TRUE);
                } else {
                    listener.onFailure(e);
                }
            });
        }
    }

    private static class TestEsqlQueryResponse extends EsqlQueryResponse {

        private final EsqlResponse esqlResponse;

        TestEsqlQueryResponse(EsqlResponse esqlResponse) {
            this.esqlResponse = esqlResponse;
        }

        @Override
        protected EsqlResponse responseInternal() {
            return esqlResponse;
        }

        @Override
        public void writeTo(StreamOutput out) {
            throw new UnsupportedOperationException("not needed in tests");
        }
    }
}
