/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlDatafeedQueryValidatorTests extends ESTestCase {

    private static final String ESQL_QUERY =
        "FROM logs | STATS avg_bytes = AVG(bytes), doc_count = COUNT(*) BY ts = BUCKET(@timestamp, 1 hour)";
    private static final String TIME_FIELD = "ts";
    private static final String SUMMARY_COUNT_FIELD = "doc_count";

    public void testRequiredColumnsPresentShouldPassValidation() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"), mockColumn(SUMMARY_COUNT_FIELD, "long"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));
        assertValidationSucceeds(harness.validator, ESQL_QUERY, null, TIME_FIELD, SUMMARY_COUNT_FIELD);
    }

    public void testNoSummaryCountFieldRequiredShouldPassValidation() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));
        assertValidationSucceeds(harness.validator, ESQL_QUERY, null, TIME_FIELD, null);
    }

    public void testMissingTimeFieldShouldFailValidation() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        Exception failure = assertValidationFails(harness.validator, ESQL_QUERY, null, TIME_FIELD, null);

        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("final ES|QL output"));
        assertThat(failure.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testMissingSummaryCountFieldShouldFailValidation() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        Exception failure = assertValidationFails(harness.validator, ESQL_QUERY, null, TIME_FIELD, SUMMARY_COUNT_FIELD);

        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(failure.getMessage(), containsString("numeric count column"));
        assertThat(failure.getMessage(), containsString("disable delayed-data checking"));
    }

    public void testIndexNotFoundShouldSucceedValidation() {
        ValidatorHarness harness = new ValidatorHarness(new IndexNotFoundException("logs"));
        assertValidationSucceeds(harness.validator, ESQL_QUERY, null, TIME_FIELD, null);
    }

    public void testEsqlUnknownIndexVerificationExceptionShouldSucceedValidation() {
        ValidatorHarness harness = new ValidatorHarness(new VerificationException("Unknown index [logs]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            SUMMARY_COUNT_FIELD,
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected success for missing index", e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testEsqlUnknownIndexInVerifierProblemListShouldSucceedValidation() {
        ValidatorHarness harness = new ValidatorHarness(new VerificationException("Found 1 problem\nline 1:1: Unknown index [logs-new]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            "FROM logs-new",
            null,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected success for missing index", e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testEsqlUnknownColumnVerificationExceptionShouldPropagate() {
        VerificationException unknownColumn = new VerificationException("Found 1 problem\nline 1:30: Unknown column [bucket]");
        ValidatorHarness harness = new ValidatorHarness(unknownColumn);

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> fail("expected failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(unknownColumn));
    }

    public void testNoMatchingProjectShouldDeferValidation() {
        ValidatorHarness harness = new ValidatorHarness(new NoMatchingProjectException("_alias:*"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            "_alias:*",
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected deferral for NoMatchingProjectException", e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testOtherExecutionFailureShouldPropagateFromValidation() {
        RuntimeException boom = new RuntimeException("query syntax error");
        ValidatorHarness harness = new ValidatorHarness(boom);

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> fail("expected failure"), failure::set)
        );

        assertThat(failure.get(), notNullValue());
        assertThat(failure.get().getMessage(), containsString("query syntax error"));
    }

    public void testValidationQueryShouldAppendLimitZeroWithoutInjectingCountColumn() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"), mockColumn(SUMMARY_COUNT_FIELD, "long"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            SUMMARY_COUNT_FIELD,
            ActionListener.wrap(ok -> {}, e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(harness.capturedQuery, equalTo(ESQL_QUERY + " | LIMIT 0"));
    }

    public void testTrailingLineCommentShouldAppendLimitZeroOnNewLineForValidation() {
        ValidatorHarness harness = new ValidatorHarness(buildResponse(List.of(mockColumn(TIME_FIELD, "date"))));

        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            "FROM logs | STATS c = COUNT(*) BY ts = BUCKET(@timestamp, 1h) // hourly",
            null,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> {}, e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(harness.capturedQuery, equalTo("FROM logs | STATS c = COUNT(*) BY ts = BUCKET(@timestamp, 1h) // hourly\n | LIMIT 0"));
    }

    public void testProjectRoutingShouldReachValidationQuery() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        harness.validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            "_alias:_origin",
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> {}, e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(harness.capturedRouting, equalTo("_alias:_origin"));
    }

    public void testMintAccessValidationShouldSucceed() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            "_alias:_origin",
            ActionListener.wrap(ignored -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(succeeded.get(), is(true));
        assertThat(harness.capturedQuery, equalTo(ESQL_QUERY + " | LIMIT 0"));
        assertThat(harness.capturedRouting, equalTo("_alias:_origin"));
    }

    public void testMintAccessWithTrailingLineCommentShouldAppendLimitZeroOnNewLine() {
        ValidatorHarness harness = new ValidatorHarness(buildResponse(List.of(mockColumn(TIME_FIELD, "date"))));

        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            "FROM logs | WHERE a > 1 // filter",
            null,
            ActionListener.wrap(ignored -> {}, e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(harness.capturedQuery, equalTo("FROM logs | WHERE a > 1 // filter\n | LIMIT 0"));
    }

    public void testMintAccessWithNoMatchingProjectShouldDefer() {
        ValidatorHarness harness = new ValidatorHarness(new NoMatchingProjectException("_alias:*"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            "_alias:*",
            ActionListener.wrap(ignored -> succeeded.set(true), e -> {
                throw new AssertionError("expected deferral", e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testMintAccessWithIndexNotFoundShouldDefer() {
        ValidatorHarness harness = new ValidatorHarness(new IndexNotFoundException("logs"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            ActionListener.wrap(ignored -> succeeded.set(true), e -> {
                throw new AssertionError("expected deferral", e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testMintAccessWithEsqlUnknownIndexVerificationExceptionShouldDefer() {
        ValidatorHarness harness = new ValidatorHarness(new VerificationException("Unknown index [logs]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            ActionListener.wrap(ignored -> succeeded.set(true), e -> {
                throw new AssertionError("expected deferral", e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testMintAccessWithOtherFailureShouldPropagate() {
        RuntimeException securityFailure = new RuntimeException("auth failure");
        ValidatorHarness harness = new ValidatorHarness(securityFailure);

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            ActionListener.wrap(ignored -> fail("expected failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(securityFailure));
    }

    public void testDateSourceTimeFieldShouldPassValidation() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
        assertThat(harness.capturedQuery, equalTo("FROM logs | KEEP ??sourceTimeField | LIMIT 0"));
        assertThat(
            harness.capturedParams,
            equalTo(
                List.of(
                    new EsqlQueryRequestBuilder.EsqlQueryParam(
                        "sourceTimeField",
                        "@timestamp",
                        EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER
                    )
                )
            )
        );
    }

    public void testDateNanosSourceTimeFieldShouldPassValidation() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date_nanos"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testWrongTypeSourceTimeFieldShouldFailValidation() {
        List<ColumnInfo> columns = List.of(mockColumn("bucket", "keyword"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "bucket",
            ActionListener.wrap(ok -> fail("expected failure"), failure::set),
            "datafeed-1"
        );

        assertThat(failure.get(), instanceOf(IllegalArgumentException.class));
        assertThat(failure.get().getMessage(), containsString("source_time_field [bucket]"));
        assertThat(failure.get().getMessage(), containsString("type [keyword]"));
        assertThat(failure.get().getMessage(), containsString("ES|QL datafeed [datafeed-1] source_time_field"));
        assertThat(failure.get().getMessage(), containsString("STATS, BUCKET, or EVAL"));
    }

    public void testForbiddenSourceTimeFieldFailureShouldPropagateStatus() {
        ElasticsearchStatusException forbidden = new ElasticsearchStatusException("access denied", RestStatus.FORBIDDEN);
        ValidatorHarness harness = new ValidatorHarness(forbidden);

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "@timestamp",
            ActionListener.wrap(ok -> fail("expected failure"), failure::set),
            "datafeed-1"
        );

        assertThat(failure.get(), sameInstance(forbidden));
        assertThat(ExceptionsHelper.status(failure.get()), equalTo(RestStatus.FORBIDDEN));
    }

    public void testTooManyRequestsSourceTimeFieldFailureShouldPropagateStatus() {
        ElasticsearchStatusException rateLimited = new ElasticsearchStatusException("rate limited", RestStatus.TOO_MANY_REQUESTS);
        ValidatorHarness harness = new ValidatorHarness(rateLimited);

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "@timestamp",
            ActionListener.wrap(ok -> fail("expected failure"), failure::set),
            null
        );

        assertThat(failure.get(), sameInstance(rateLimited));
        assertThat(ExceptionsHelper.status(failure.get()), equalTo(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testLookupJoinUnknownIndexShouldDeferExistenceFailure() {
        VerificationException lookupMissing = new VerificationException("Unknown index [fake-lookup]");
        assertTrue(EsqlDatafeedQueryValidator.isDeferredExistenceFailure(lookupMissing));
    }

    public void testUnknownColumnSourceTimeFieldShouldFailValidation() {
        VerificationException unknownColumn = new VerificationException("Found 1 problem\nline 1:20: Unknown column [bucket]");
        ValidatorHarness harness = new ValidatorHarness(unknownColumn);

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "bucket",
            ActionListener.wrap(ok -> fail("expected failure"), failure::set),
            null
        );

        assertThat(failure.get(), instanceOf(IllegalArgumentException.class));
        assertThat(failure.get().getMessage(), containsString("source_time_field [bucket]"));
        assertThat(failure.get().getMessage(), containsString("Unknown column [bucket]"));
        assertThat(failure.get().getMessage(), containsString("STATS, BUCKET, or EVAL"));
    }

    public void testIndexNotFoundSourceTimeFieldShouldSucceedValidation() {
        ValidatorHarness harness = new ValidatorHarness(new IndexNotFoundException("logs"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected success for missing index", e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testEsqlUnknownIndexSourceTimeFieldVerificationExceptionShouldSucceedValidation() {
        ValidatorHarness harness = new ValidatorHarness(new VerificationException("Found 1 problem\nline 1:1: Unknown index [logs]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected success for missing index", e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testNoMatchingProjectSourceTimeFieldShouldDeferValidation() {
        ValidatorHarness harness = new ValidatorHarness(new NoMatchingProjectException("_alias:*"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            "_alias:*",
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected deferral for NoMatchingProjectException", e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testSourceTimeFieldValidationShouldProbeOnlyLeadingFromCommand() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            "FROM logs | STATS bucket = BUCKET(@timestamp, 1h) BY bucket",
            null,
            "@timestamp",
            ActionListener.wrap(ok -> {}, e -> {
                throw new AssertionError(e);
            }),
            null
        );

        assertThat(harness.capturedQuery, equalTo("FROM logs | KEEP ??sourceTimeField | LIMIT 0"));
    }

    public void testLeadingCommandEndingInLineCommentShouldAppendKeepOnNewLineForSourceTimeField() {
        ValidatorHarness harness = new ValidatorHarness(buildResponse(List.of(mockColumn("@timestamp", "date"))));

        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            "FROM logs // web tier\n| STATS c = COUNT(*)",
            null,
            "@timestamp",
            ActionListener.wrap(ok -> {}, e -> {
                throw new AssertionError(e);
            }),
            null
        );

        assertThat(harness.capturedQuery, equalTo("FROM logs // web tier\n | KEEP ??sourceTimeField | LIMIT 0"));
    }

    public void testNonFromLeadingQueryShouldSkipSourceTimeFieldValidation() {
        ValidatorHarness harness = new ValidatorHarness(new RuntimeException("should not be invoked"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            "ROW x = 1",
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected skip for a non-FROM-leading query", e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
        assertThat(harness.capturedQuery, nullValue());
    }

    // A leading line comment before FROM must not defeat the
    // FROM-detection scan. extractLeadingCommand() returns the comment together with the FROM command (it
    // only skips comments while looking for the next top-level pipe, not when reporting the leading text),
    // so the probe-eligibility check must skip past the comment itself before comparing against "FROM".
    public void testLineCommentPrefixedQueryShouldProbeSourceTimeField() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            "// note\nFROM logs",
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
        assertThat(harness.capturedQuery, equalTo("// note\nFROM logs | KEEP ??sourceTimeField | LIMIT 0"));
    }

    // Same as above but for a leading block comment.
    public void testBlockCommentPrefixedQueryShouldProbeSourceTimeField() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            "/* x */ FROM logs",
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
        assertThat(harness.capturedQuery, equalTo("/* x */ FROM logs | KEEP ??sourceTimeField | LIMIT 0"));
    }

    // TS is a real ES|QL source command (time-series source);
    // and EsqlDataExtractor#fetchSourceRangeSummary already reuses extractLeadingCommand() generically (not
    // FROM-specific) at runtime, so a TS-leading datafeed query's runtime path already works. The PUT-time
    // validator must accept it too rather than silently skipping the source_time_field check.
    public void testTsLeadingQueryShouldProbeSourceTimeField() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        ValidatorHarness harness = new ValidatorHarness(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateSourceTimeField(
            null,
            Collections.emptyMap(),
            "TS metrics-* | STATS avg(value) BY host",
            null,
            "@timestamp",
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            }),
            null
        );

        assertThat(succeeded.get(), is(true));
        assertThat(harness.capturedQuery, equalTo("TS metrics-* | KEEP ??sourceTimeField | LIMIT 0"));
    }

    public void testNullEmittedTimeInSourceWindowShouldUseMessages() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.validateEmittedTimeValueInSourceWindow("job-1", TIME_FIELD, null, 100L, 200L)
        );
        assertThat(
            e.getMessage(),
            equalTo(Messages.getMessage(Messages.DATAFEED_ESQL_EMITTED_TIME_NULL, "job-1", TIME_FIELD, 100L, 200L, null))
        );
    }

    public void testAllRequiredColumnsPresentShouldSucceedCheck() {
        List<ColumnInfo> columns = List.of(
            mockColumn(TIME_FIELD, "date"),
            mockColumn(SUMMARY_COUNT_FIELD, "long"),
            mockColumn("other", "keyword")
        );
        EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null);
    }

    public void testNoSummaryCountFieldRequiredShouldSucceedColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null, null);
    }

    public void testMissingTimeFieldShouldThrowFromColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null, null)
        );
        assertThat(e.getMessage(), containsString("final ES|QL output"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testMissingTimeFieldWithDatafeedIdShouldNameDatafeedInColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null, "esql-datafeed")
        );
        assertThat(e.getMessage(), containsString("ES|QL datafeed [esql-datafeed] query"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testMissingSummaryCountFieldShouldThrowFromColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null)
        );
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(e.getMessage(), containsString("numeric count column"));
    }

    public void testBothRequiredColumnsMissingShouldListBothInError() {
        List<ColumnInfo> columns = List.of(mockColumn("unrelated", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null)
        );
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(e.getMessage(), containsString("final ES|QL output"));
        assertThat(e.getMessage(), containsString("numeric count column"));
    }

    public void testEmptyColumnsShouldThrowFromColumnCheck() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(List.of(), TIME_FIELD, null, null)
        );
        assertThat(e.getMessage(), containsString(TIME_FIELD));
    }

    public void testConflictingOuterClausesShouldAddActionableWarnings() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        try {
            EsqlDatafeedQueryValidator.warnForConflictingOuterClauses(
                "datafeed-1",
                "FROM logs | WHERE ts >= 0 | SORT ts DESC | LIMIT 20",
                TIME_FIELD
            );

            assertWarnings(
                false,
                List.of(
                    allOf(
                        containsString("ES|QL datafeed [datafeed-1] query contains an outer WHERE clause"),
                        containsString("ML owns the request window")
                    ),
                    allOf(
                        containsString("ES|QL datafeed [datafeed-1] query contains an outer SORT clause"),
                        containsString("ML owns the request order")
                    ),
                    allOf(
                        containsString("ES|QL datafeed [datafeed-1] query contains an outer LIMIT clause"),
                        containsString("ML owns the safety ceiling")
                    )
                )
            );
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    public void testRawKeepAndLexicalFalsePositivesShouldNotWarn() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        HeaderWarning.setThreadContext(threadContext);
        try {
            EsqlDatafeedQueryValidator.warnForConflictingOuterClauses(
                "datafeed-1",
                "FROM logs | KEEP ts, value | EVAL note = \"WHERE ts | SORT ts | LIMIT 20\" "
                    + "| EVAL triple_note = \"\"\"WHERE ts | SORT ts | LIMIT 20\"\"\" "
                    + "| KEEP `WHERE ts`, value /* WHERE ts | SORT ts | LIMIT 20 /* WHERE ts */ */ "
                    + "// WHERE ts | SORT ts | LIMIT 20\n | FORK (WHERE ts > 0 | SORT ts | LIMIT 20)",
                TIME_FIELD
            );

            assertWarnings();
        } finally {
            HeaderWarning.removeThreadContext(threadContext);
        }
    }

    public void testDelayedCheckEnabledWithFieldSetShouldReturnRequiredSummaryCountField() {
        Job job = buildJob("job-1", SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig());

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), equalTo(SUMMARY_COUNT_FIELD));
    }

    public void testDelayedCheckDisabledShouldReturnNullRequiredSummaryCountField() {
        Job job = buildJob("job-1", SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), nullValue());
    }

    public void testNoSummaryCountFieldSetShouldReturnNullRequiredSummaryCountField() {
        Job job = buildJob("job-1", null);
        DatafeedConfig datafeed = buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig());

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), nullValue());
    }

    public void testNullDelayedDataCheckConfigShouldReturnNullRequiredSummaryCountField() {
        Job job = buildJob("job-1", SUMMARY_COUNT_FIELD);
        DatafeedConfig datafeed = buildDatafeedWithNullDelayedDataCheckConfig("datafeed-1", "job-1");

        assertThat(EsqlDatafeedQueryValidator.requiredSummaryCountField(datafeed, job), nullValue());
    }

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

    public void testRemoteIndexShouldFailRemoteClusterSourceRejection() {
        ElasticsearchStatusException e = expectRejected("FROM remote:logs-* | STATS c = COUNT(*) BY BUCKET(@timestamp, 1h)");
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), containsString("do not support remote cluster sources"));
        assertThat(e.getMessage(), containsString("[remote:logs-*]"));
        assertThat(e.getMessage(), containsString("query local indices only"));
    }

    public void testRemoteIndexInCommaListShouldFailRemoteClusterSourceRejection() {
        assertThat(expectRejected("FROM a,remote:b").getMessage(), containsString("[remote:b]"));
        assertThat(expectRejected("FROM a , b , east:c METADATA _id").getMessage(), containsString("[east:c]"));
    }

    public void testWildcardClusterShouldFailRemoteClusterSourceRejection() {
        assertThat(expectRejected("FROM *:logs-*").getMessage(), containsString("[*:logs-*]"));
        assertThat(expectRejected("FROM cl*:logs-*").getMessage(), containsString("[cl*:logs-*]"));
    }

    public void testQuotedClusterPrefixShouldFailRemoteClusterSourceRejection() {
        assertThat(expectRejected("FROM \"east:logs-*\"").getMessage(), containsString("[east:logs-*]"));
        assertThat(expectRejected("FROM a, \"\"\"east:logs\"\"\"").getMessage(), containsString("[east:logs]"));
    }

    public void testTimeSeriesSourceShouldFailRemoteClusterSourceRejection() {
        assertThat(expectRejected("TS remote:metrics-* | STATS AVG(cpu)").getMessage(), containsString("[remote:metrics-*]"));
        assertThat(expectRejected("ts metrics-*,remote:metrics-*").getMessage(), containsString("[remote:metrics-*]"));
    }

    public void testExcludedRemoteIndexShouldFailRemoteClusterSourceRejection() {
        assertThat(expectRejected("FROM logs-*,-remote:logs-old").getMessage(), containsString("[-remote:logs-old]"));
    }

    public void testLeadingCommentWithRemoteIndexShouldStillFailRemoteClusterSourceRejection() {
        assertThat(expectRejected("// source\nFROM /* inline */ remote:logs").getMessage(), containsString("[remote:logs]"));
        assertThat(expectRejected("FROM a, // next\n remote:b").getMessage(), containsString("[remote:b]"));
    }

    public void testLocalSourcesShouldAcceptRemoteClusterSourceRejection() {
        for (String query : List.of(
            "FROM logs-*",
            "FROM logs-* METADATA _id",
            "FROM a, b METADATA _id, _index",
            "TS metrics-* | STATS AVG(cpu) BY BUCKET(@timestamp, 1h)",
            "FROM \"logs-*\"",
            "FROM <logs-{now/d}>",
            "FROM \"<logs-{now{yyyy.MM.dd|+12:00}}>\"",
            "FROM logs-*,-logs-old"
        )) {
            EsqlDatafeedQueryValidator.rejectRemoteClusterSources(query, false);
        }
    }

    public void testSelectorSyntaxShouldAcceptRemoteClusterSourceRejection() {
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM idx::failures", false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM idx::data, other::failures METADATA _id", false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM \"idx::failures\"", false);
    }

    public void testColonOutsideSourceListShouldAcceptRemoteClusterSourceRejection() {
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("// remote:logs\nFROM logs-*", false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM /* east:logs */ logs-*", false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM logs-* // remote:logs", false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM logs-* | WHERE host == \"a:b\" | EVAL x = \"remote:idx\"", false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM logs-* METADATA _id | LOOKUP JOIN remote:lookup ON k", false);
    }

    public void testOriginQualifierShouldAcceptRemoteClusterSourceRejection() {
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM _origin:logs-*", false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM logs-*,-_origin:logs-old", false);
    }

    public void testCrossProjectEnabledWithProjectQualifiedSourcesShouldAcceptRemoteClusterSourceRejection() {
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM linked-project-1:logs-*", true);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("FROM *:logs-*", true);
    }

    public void testNoQueryOrNonSourceQueryShouldAcceptRemoteClusterSourceRejection() {
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources(null, false);
        EsqlDatafeedQueryValidator.rejectRemoteClusterSources("ROW a = \"remote:idx\"", false);
    }

    private static ElasticsearchStatusException expectRejected(String query) {
        return expectThrows(ElasticsearchStatusException.class, () -> EsqlDatafeedQueryValidator.rejectRemoteClusterSources(query, false));
    }

    private void assertValidationSucceeds(
        EsqlDatafeedQueryValidator validator,
        String esqlQuery,
        String projectRouting,
        String timeField,
        String summaryCountField
    ) {
        AtomicBoolean succeeded = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateQuery(
            null,
            Collections.emptyMap(),
            esqlQuery,
            projectRouting,
            timeField,
            summaryCountField,
            ActionListener.wrap(ok -> succeeded.set(true), failure::set)
        );
        assertThat(succeeded.get(), is(true));
        assertThat(failure.get(), nullValue());
    }

    private Exception assertValidationFails(
        EsqlDatafeedQueryValidator validator,
        String esqlQuery,
        String projectRouting,
        String timeField,
        String summaryCountField
    ) {
        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateQuery(
            null,
            Collections.emptyMap(),
            esqlQuery,
            projectRouting,
            timeField,
            summaryCountField,
            ActionListener.wrap(ok -> fail("expected validation failure"), failure::set)
        );
        assertThat(failure.get(), notNullValue());
        return failure.get();
    }

    private static final class ValidatorHarness {
        final EsqlDatafeedQueryValidator validator;
        String capturedQuery;
        String capturedRouting;
        List<EsqlQueryRequestBuilder.EsqlQueryParam> capturedParams;

        ValidatorHarness(EsqlQueryResponse response) {
            this(response, null);
        }

        ValidatorHarness(Exception failure) {
            this(null, failure);
        }

        private ValidatorHarness(EsqlQueryResponse response, Exception failure) {
            validator = new EsqlDatafeedQueryValidator((client, query, filter, headers, projectRouting, params, listener) -> {
                capturedQuery = query;
                capturedRouting = projectRouting;
                capturedParams = params;
                if (failure != null) {
                    listener.onFailure(failure);
                } else {
                    listener.onResponse(response);
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
