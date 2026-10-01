/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
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
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.allOf;
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

        validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            SUMMARY_COUNT_FIELD,
            ActionListener.wrap(ok -> succeeded.set(true), failure::set)
        );

        assertThat(succeeded.get(), is(true));
        assertThat(failure.get(), equalTo(null));
    }

    public void testValidateQueryGivenNoSummaryCountFieldRequired() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testValidateQueryGivenMissingTimeFieldFails() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> fail("expected failure"), failure::set)
        );

        assertThat(failure.get(), instanceOf(IllegalArgumentException.class));
        assertThat(failure.get().getMessage(), containsString("final ES|QL output"));
        assertThat(failure.get().getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testValidateQueryGivenMissingSummaryCountFieldFails() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            SUMMARY_COUNT_FIELD,
            ActionListener.wrap(ok -> fail("expected failure"), failure::set)
        );

        assertThat(failure.get(), instanceOf(IllegalArgumentException.class));
        assertThat(failure.get().getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(failure.get().getMessage(), containsString("numeric count column"));
        assertThat(failure.get().getMessage(), containsString("disable delayed-data checking"));
    }

    public void testValidateQueryGivenIndexNotFoundSucceeds() {
        TestValidator validator = new TestValidator(new IndexNotFoundException("logs"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateQuery(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            TIME_FIELD,
            null,
            ActionListener.wrap(ok -> succeeded.set(true), e -> {
                throw new AssertionError("expected success for missing index", e);
            })
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testValidateQueryGivenEsqlUnknownIndexVerificationExceptionSucceeds() {
        TestValidator validator = new TestValidator(new VerificationException("Unknown index [logs]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateQuery(
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

    public void testValidateQueryGivenEsqlUnknownIndexInVerifierProblemListSucceeds() {
        TestValidator validator = new TestValidator(new VerificationException("Found 1 problem\nline 1:1: Unknown index [logs-new]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateQuery(
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

    public void testValidateQueryGivenEsqlUnknownColumnVerificationExceptionPropagates() {
        VerificationException unknownColumn = new VerificationException("Found 1 problem\nline 1:30: Unknown column [bucket]");
        TestValidator validator = new TestValidator(unknownColumn);

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateQuery(
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

    public void testValidateQueryGivenNoMatchingProjectIsDeferred() {
        TestValidator validator = new TestValidator(new NoMatchingProjectException("_alias:*"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateQuery(
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

    public void testValidateQueryGivenOtherExecutionFailurePropagates() {
        RuntimeException boom = new RuntimeException("query syntax error");
        TestValidator validator = new TestValidator(boom);

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateQuery(
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

    public void testValidateQueryAppendsLimitZeroWithoutInjectingCountColumn() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"), mockColumn(SUMMARY_COUNT_FIELD, "long"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        validator.validateQuery(
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

        assertThat(validator.capturedQuery, equalTo(ESQL_QUERY + " | LIMIT 0"));
    }

    public void testValidateQueryGivenTrailingLineCommentShouldAppendLimitZeroOnNewLine() {
        TestValidator validator = new TestValidator(buildResponse(List.of(mockColumn(TIME_FIELD, "date"))));

        validator.validateQuery(
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

        assertThat(validator.capturedQuery, equalTo("FROM logs | STATS c = COUNT(*) BY ts = BUCKET(@timestamp, 1h) // hourly\n | LIMIT 0"));
    }

    public void testValidateQueryPassesProjectRouting() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        validator.validateQuery(
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

        assertThat(validator.capturedRouting, equalTo("_alias:_origin"));
    }

    public void testValidateAccessForMintSucceeds() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            "_alias:_origin",
            ActionListener.wrap(ignored -> succeeded.set(true), e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(succeeded.get(), is(true));
        assertThat(validator.capturedQuery, equalTo(ESQL_QUERY + " | LIMIT 0"));
        assertThat(validator.capturedRouting, equalTo("_alias:_origin"));
    }

    public void testValidateAccessForMintGivenTrailingLineCommentShouldAppendLimitZeroOnNewLine() {
        TestValidator validator = new TestValidator(buildResponse(List.of(mockColumn(TIME_FIELD, "date"))));

        validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            "FROM logs | WHERE a > 1 // filter",
            null,
            ActionListener.wrap(ignored -> {}, e -> {
                throw new AssertionError(e);
            })
        );

        assertThat(validator.capturedQuery, equalTo("FROM logs | WHERE a > 1 // filter\n | LIMIT 0"));
    }

    public void testValidateAccessForMintNoMatchingProjectIsDeferred() {
        TestValidator validator = new TestValidator(new NoMatchingProjectException("_alias:*"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateAccessForMint(
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

    public void testValidateAccessForMintIndexNotFoundIsDeferred() {
        TestValidator validator = new TestValidator(new IndexNotFoundException("logs"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateAccessForMint(
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

    public void testValidateAccessForMintGivenEsqlUnknownIndexVerificationExceptionIsDeferred() {
        TestValidator validator = new TestValidator(new VerificationException("Unknown index [logs]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateAccessForMint(
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

    public void testValidateAccessForMintOtherFailurePropagates() {
        RuntimeException securityFailure = new RuntimeException("auth failure");
        TestValidator validator = new TestValidator(securityFailure);

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            ActionListener.wrap(ignored -> fail("expected failure"), failure::set)
        );

        assertThat(failure.get(), equalTo(securityFailure));
    }

    public void testValidateSourceTimeFieldGivenDateSucceeds() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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
        assertThat(validator.capturedQuery, equalTo("FROM logs | KEEP ??sourceTimeField | LIMIT 0"));
        assertThat(
            validator.capturedParams,
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

    public void testValidateSourceTimeFieldGivenDateNanosSucceeds() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date_nanos"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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

    public void testValidateSourceTimeFieldGivenWrongTypeFails() {
        List<ColumnInfo> columns = List.of(mockColumn("bucket", "keyword"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateSourceTimeField(
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
        assertThat(failure.get().getMessage(), containsString("for datafeed [datafeed-1]"));
        assertThat(failure.get().getMessage(), containsString("STATS, BUCKET, or EVAL"));
    }

    public void testValidateSourceTimeFieldGivenUnknownColumnFails() {
        RuntimeException unknownColumn = new RuntimeException("Found 1 problem\nline 1:20: Unknown column [bucket]");
        TestValidator validator = new TestValidator(unknownColumn);

        AtomicReference<Exception> failure = new AtomicReference<>();
        validator.validateSourceTimeField(
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

    public void testValidateSourceTimeFieldGivenIndexNotFoundSucceeds() {
        TestValidator validator = new TestValidator(new IndexNotFoundException("logs"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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

    public void testValidateSourceTimeFieldGivenEsqlUnknownIndexVerificationExceptionSucceeds() {
        TestValidator validator = new TestValidator(new VerificationException("Found 1 problem\nline 1:1: Unknown index [logs]"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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

    public void testValidateSourceTimeFieldGivenNoMatchingProjectIsDeferred() {
        TestValidator validator = new TestValidator(new NoMatchingProjectException("_alias:*"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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

    public void testValidateSourceTimeFieldProbesOnlyTheLeadingFromCommand() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        validator.validateSourceTimeField(
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

        assertThat(validator.capturedQuery, equalTo("FROM logs | KEEP ??sourceTimeField | LIMIT 0"));
    }

    public void testValidateSourceTimeFieldGivenLeadingCommandEndingInLineCommentShouldAppendKeepOnNewLine() {
        TestValidator validator = new TestValidator(buildResponse(List.of(mockColumn("@timestamp", "date"))));

        validator.validateSourceTimeField(
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

        assertThat(validator.capturedQuery, equalTo("FROM logs // web tier\n | KEEP ??sourceTimeField | LIMIT 0"));
    }

    public void testValidateSourceTimeFieldGivenNonFromLeadingQuerySkips() {
        TestValidator validator = new TestValidator(new RuntimeException("should not be invoked"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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
        assertThat(validator.capturedQuery, nullValue());
    }

    // Regression test for elastic-workspace-g2sz.2: a leading line comment before FROM must not defeat the
    // FROM-detection scan. extractLeadingCommand() returns the comment together with the FROM command (it
    // only skips comments while looking for the next top-level pipe, not when reporting the leading text),
    // so the probe-eligibility check must skip past the comment itself before comparing against "FROM".
    public void testValidateSourceTimeFieldGivenLineCommentPrefixedQueryProbes() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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
        assertThat(validator.capturedQuery, equalTo("// note\nFROM logs | KEEP ??sourceTimeField | LIMIT 0"));
    }

    // Same as above but for a leading block comment.
    public void testValidateSourceTimeFieldGivenBlockCommentPrefixedQueryProbes() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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
        assertThat(validator.capturedQuery, equalTo("/* x */ FROM logs | KEEP ??sourceTimeField | LIMIT 0"));
    }

    // Regression test for elastic-workspace-g2sz.2: TS is a real ES|QL source command (time-series source),
    // and EsqlDataExtractor#fetchSourceRangeSummary already reuses extractLeadingCommand() generically (not
    // FROM-specific) at runtime, so a TS-leading datafeed query's runtime path already works. The PUT-time
    // validator must accept it too rather than silently skipping the source_time_field check.
    public void testValidateSourceTimeFieldGivenTsLeadingQueryProbes() {
        List<ColumnInfo> columns = List.of(mockColumn("@timestamp", "date"));
        TestValidator validator = new TestValidator(buildResponse(columns));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        validator.validateSourceTimeField(
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
        assertThat(validator.capturedQuery, equalTo("TS metrics-* | KEEP ??sourceTimeField | LIMIT 0"));
    }

    public void testCheckRequiredColumnsGivenAllPresentSucceeds() {
        List<ColumnInfo> columns = List.of(
            mockColumn(TIME_FIELD, "date"),
            mockColumn(SUMMARY_COUNT_FIELD, "long"),
            mockColumn("other", "keyword")
        );
        EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null);
    }

    public void testCheckRequiredColumnsGivenNoSummaryCountFieldRequiredSucceeds() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null, null);
    }

    public void testCheckRequiredColumnsGivenMissingTimeFieldThrows() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null, null)
        );
        assertThat(e.getMessage(), containsString("final ES|QL output"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testCheckRequiredColumnsGivenMissingTimeFieldAndDatafeedIdNamesTheDatafeed() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, null, "esql-datafeed")
        );
        assertThat(e.getMessage(), containsString("for datafeed [esql-datafeed]"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testCheckRequiredColumnsGivenMissingSummaryCountFieldThrows() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlDatafeedQueryValidator.checkRequiredColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null)
        );
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(e.getMessage(), containsString("numeric count column"));
    }

    public void testCheckRequiredColumnsGivenBothMissingListsBothInError() {
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

    public void testCheckRequiredColumnsGivenEmptyColumnsThrows() {
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
     * Test subclass of {@link EsqlDatafeedQueryValidator} that overrides {@link #executeEsqlQueryAsync}
     * to avoid the {@code SharedSecrets}/esql-plugin dependency absent from the ml plugin test classpath.
     * Instead of building and sending a real ESQL request it either returns a pre-built response or
     * simulates a query execution failure, and captures the query string and project routing for assertion.
     */
    private class TestValidator extends EsqlDatafeedQueryValidator {

        private final EsqlQueryResponse cannedResponse;
        private final Exception cannedFailure;
        String capturedQuery;
        String capturedRouting;
        List<EsqlQueryRequestBuilder.EsqlQueryParam> capturedParams;

        TestValidator(EsqlQueryResponse response) {
            this.cannedResponse = response;
            this.cannedFailure = null;
        }

        TestValidator(Exception failure) {
            this.cannedResponse = null;
            this.cannedFailure = failure;
        }

        @Override
        protected void executeEsqlQueryAsync(
            Client client,
            String query,
            Map<String, String> headers,
            String projectRouting,
            List<EsqlQueryRequestBuilder.EsqlQueryParam> params,
            ActionListener<EsqlQueryResponse> listener
        ) {
            capturedQuery = query;
            capturedRouting = projectRouting;
            capturedParams = params;
            if (cannedFailure != null) {
                listener.onFailure(cannedFailure);
            } else {
                listener.onResponse(cannedResponse);
            }
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
