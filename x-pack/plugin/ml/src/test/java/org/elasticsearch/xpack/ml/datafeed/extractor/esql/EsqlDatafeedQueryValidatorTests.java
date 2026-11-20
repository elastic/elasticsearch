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
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.ExecutionMetadata;
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
import org.elasticsearch.xpack.ml.datafeed.crossproject.NoMatchingProjectTestMessages;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
        "FROM logs | STATS avg_bytes = AVG(bytes), doc_count = COUNT(*) BY @timestamp = BUCKET(@timestamp, 1 hour)";
    private static final String TIME_FIELD = "@timestamp";
    private static final String SUMMARY_COUNT_FIELD = "doc_count";

    public void testMappedTimeFieldShouldPassValidation() {
        assertValidationSucceeds(harness(row("@timestamp", "date", "time", "@timestamp", "mapped")), validation());
    }

    public void testDateNanosTimeFieldShouldPassValidation() {
        assertValidationSucceeds(harness(row("@timestamp", "date_nanos", "time", "@timestamp", "mapped")), validation());
    }

    public void testComputedBucketNamedAfterSourceFieldShouldPassValidation() {
        ValidatorHarness harness = harness(
            row("@timestamp", "date", "time", null, "computed", "1 hour"),
            row(SUMMARY_COUNT_FIELD, "long", null, null, "computed")
        );
        assertValidationSucceeds(harness, validation().summaryCountField(SUMMARY_COUNT_FIELD));
    }

    public void testRenamedTimeFieldWithMatchingSourceTimeFieldShouldPassValidation() {
        assertValidationSucceeds(
            harness(row("ts", "date", "time", "@timestamp", "renamed")),
            validation().timeField("ts").sourceTimeField("@timestamp")
        );
    }

    public void testRenamedTimeFieldWithMismatchingSourceTimeFieldShouldFailValidation() {
        Exception failure = assertValidationFails(
            harness(row("ts", "date", "time", "@timestamp", "renamed")),
            validation().timeField("ts").sourceTimeField("other")
        );
        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("source_time_field [other] does not match the source field [@timestamp]"));
    }

    public void testMappedTimeFieldWithMismatchingSourceTimeFieldShouldFailValidation() {
        Exception failure = assertValidationFails(
            harness(row("@timestamp", "date", "time", "@timestamp", "mapped")),
            validation().sourceTimeField("event.created")
        );
        assertThat(failure.getMessage(), containsString("does not match the source field [@timestamp]"));
    }

    public void testComputedBucketNamedDifferentlyFromSourceTimeFieldShouldFailWithHint() {
        Exception failure = assertValidationFails(
            harness(row("hour", "date", "time", null, "computed", "1 hour")),
            validation().timeField("hour").sourceTimeField("@timestamp")
        );
        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("Name the bucket after its source field: BY @timestamp = BUCKET(@timestamp, ...)"));
    }

    public void testComputedDateWithoutIntervalShouldFailValidation() {
        Exception failure = assertValidationFails(harness(row("@timestamp", "date", null, null, "computed")), validation());
        assertThat(failure.getMessage(), containsString("is not a date bucket."));
    }

    public void testComputedDateWithIntervalButWithoutTimeRoleShouldFailValidation() {
        Exception failure = assertValidationFails(harness(row("@timestamp", "date", null, null, "computed", "1 hour")), validation());
        assertThat(failure.getMessage(), containsString("is not a date bucket."));
    }

    public void testUnsupportedTimeColumnOriginShouldFailValidation() {
        Exception failure = assertValidationFails(harness(row("@timestamp", "date", null, null, "metadata")), validation());
        assertThat(failure.getMessage(), containsString("has origin [metadata]"));
    }

    public void testNonDateTimeColumnShouldFailValidation() {
        Exception failure = assertValidationFails(harness(row("@timestamp", "keyword", null, "@timestamp", "mapped")), validation());
        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("time column [@timestamp] has type [keyword]. It must be date or date_nanos."));
    }

    public void testMissingTimeColumnShouldFailValidation() {
        Exception failure = assertValidationFails(harness(row("other_field", "keyword", null, "other_field", "mapped")), validation());

        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString("final ES|QL output"));
        assertThat(failure.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testMissingSummaryCountColumnShouldFailValidation() {
        Exception failure = assertValidationFails(
            harness(row("@timestamp", "date", "time", "@timestamp", "mapped")),
            validation().summaryCountField(SUMMARY_COUNT_FIELD)
        );

        assertThat(failure, instanceOf(IllegalArgumentException.class));
        assertThat(failure.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(failure.getMessage(), containsString("numeric count column"));
        assertThat(failure.getMessage(), containsString("disable delayed-data checking"));
    }

    public void testNonIntegerSummaryCountColumnShouldFailValidation() {
        Exception failure = assertValidationFails(
            harness(row("@timestamp", "date", "time", "@timestamp", "mapped"), row(SUMMARY_COUNT_FIELD, "double", null, null, "computed")),
            validation().summaryCountField(SUMMARY_COUNT_FIELD)
        );
        assertThat(failure.getMessage(), containsString("has type [double]"));
    }

    public void testRemoteClusterShouldFailValidation() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped")).withClusters(
            new EsqlClusterInfo("", "logs-*", EsqlClusterInfo.Status.SUCCESSFUL),
            new EsqlClusterInfo("east", "logs-*", EsqlClusterInfo.Status.SUCCESSFUL)
        );

        Exception failure = assertValidationFails(harness, validation());

        assertThat(failure, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure).status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(failure.getMessage(), containsString("do not support remote cluster sources"));
        assertThat(failure.getMessage(), containsString("[east:logs-*]"));
        assertThat(failure.getMessage(), containsString("remote cluster [east]"));
    }

    public void testRemoteClusterShouldBeCheckedBeforeColumns() {
        ValidatorHarness harness = harness(row("other_field", "keyword", null, null, "mapped")).withClusters(
            new EsqlClusterInfo("east", "logs-*", EsqlClusterInfo.Status.SUCCESSFUL)
        );
        assertThat(assertValidationFails(harness, validation()), instanceOf(ElasticsearchStatusException.class));
    }

    public void testLocalAndOriginClustersShouldPassValidation() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped")).withClusters(
            new EsqlClusterInfo("", "logs-*", EsqlClusterInfo.Status.SUCCESSFUL),
            new EsqlClusterInfo("_origin", "logs-*", EsqlClusterInfo.Status.SUCCESSFUL)
        );
        assertValidationSucceeds(harness, validation());
    }

    public void testRemoteClusterShouldPassValidationWhenCrossProjectIsEnabled() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped")).withClusters(
            new EsqlClusterInfo("linked-project-1", "logs-*", EsqlClusterInfo.Status.SUCCESSFUL)
        );
        assertValidationSucceeds(harness, validation().crossProjectEnabled(true));
    }

    public void testIndexNotFoundShouldSucceedValidation() {
        assertValidationSucceeds(new ValidatorHarness(new IndexNotFoundException("logs")), validation());
    }

    public void testEsqlUnknownIndexVerificationExceptionShouldSucceedValidation() {
        assertValidationSucceeds(new ValidatorHarness(new VerificationException("Unknown index [logs]")), validation());
    }

    public void testEsqlUnknownIndexInVerifierProblemListShouldSucceedValidation() {
        assertValidationSucceeds(
            new ValidatorHarness(new VerificationException("Found 1 problem\nline 1:1: Unknown index [logs-new]")),
            validation().query("FROM logs-new")
        );
    }

    public void testEsqlUnknownColumnVerificationExceptionShouldPropagate() {
        VerificationException unknownColumn = new VerificationException("Found 1 problem\nline 1:30: Unknown column [bucket]");
        assertThat(assertValidationFails(new ValidatorHarness(unknownColumn), validation()), equalTo(unknownColumn));
    }

    public void testNoMatchingProjectShouldDeferValidation() {
        assertValidationSucceeds(new ValidatorHarness(new NoMatchingProjectException("_alias:*")), validation().projectRouting("_alias:*"));
    }

    public void testOtherExecutionFailureShouldPropagateFromValidation() {
        Exception failure = assertValidationFails(new ValidatorHarness(new RuntimeException("query syntax error")), validation());
        assertThat(failure.getMessage(), containsString("query syntax error"));
    }

    public void testForbiddenFailureShouldPropagateStatus() {
        ElasticsearchStatusException securityFailure = new ElasticsearchStatusException("forbidden", RestStatus.FORBIDDEN);
        Exception failure = assertValidationFails(new ValidatorHarness(securityFailure), validation());
        assertThat(failure, sameInstance(securityFailure));
        assertThat(ExceptionsHelper.status(failure), equalTo(RestStatus.FORBIDDEN));
    }

    public void testValidationQueryShouldAppendDescribeWithCcsOnlyMetadata() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped"));

        assertValidationSucceeds(harness, validation());

        assertThat(harness.capturedQuery, equalTo(ESQL_QUERY + "\n| DESCRIBE"));
        assertThat(harness.capturedQuery.endsWith("\n| DESCRIBE"), is(true));
        assertThat(harness.capturedMetadata, equalTo(ExecutionMetadata.CCS_ONLY));
        assertThat(harness.capturedParams, equalTo(List.of()));
    }

    public void testTrailingLineCommentShouldKeepDescribeOnNewLine() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped"));

        assertValidationSucceeds(harness, validation().query("FROM logs | KEEP @timestamp // hourly"));

        assertThat(harness.capturedQuery, equalTo("FROM logs | KEEP @timestamp // hourly\n\n| DESCRIBE"));
    }

    public void testProjectRoutingShouldReachValidationQuery() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped"));

        assertValidationSucceeds(harness, validation().projectRouting("_alias:_origin"));

        assertThat(harness.capturedRouting, equalTo("_alias:_origin"));
    }

    public void testMissingSourceTimeFieldShouldDefaultToTimeField() {
        assertValidationSucceeds(harness(row("@timestamp", "date", "time", "@timestamp", "mapped")), validation().sourceTimeField(null));
    }

    public void testExtraDescribeColumnsShouldNotAffectValidation() {
        EsqlResponse response = describeResponse(
            List.of(),
            List.of("name", "type", "origin", "field", "role", "future_column"),
            List.of(List.of("@timestamp", "date", "mapped", "@timestamp", "time", "x"))
        );
        assertValidationSucceeds(new ValidatorHarness(new TestEsqlQueryResponse(response)), validation());
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
            }),
            null
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
            }),
            null
        );

        assertThat(harness.capturedQuery, equalTo("FROM logs | WHERE a > 1 // filter\n | LIMIT 0"));
    }

    public void testNoProjectAfterRoutingMessageMatchesServerText() {
        NoMatchingProjectException fromServer = NoMatchingProjectTestMessages.noProjectAfterRouting("_alias:*");
        assertThat(fromServer.getMessage(), containsString(EsqlDatafeedQueryValidator.NO_PROJECT_AFTER_ROUTING_MESSAGE));
        NoMatchingProjectException explicitMissing = NoMatchingProjectTestMessages.explicitMissingQualifiedProject();
        assertThat(explicitMissing.getMessage(), containsString("No such project"));
    }

    public void testMintAccessWithNoProjectAfterRoutingShouldDefer() {
        ValidatorHarness harness = new ValidatorHarness(NoMatchingProjectTestMessages.noProjectAfterRouting("_alias:*"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            "_alias:*",
            ActionTestUtils.assertNoFailureListener(ignored -> succeeded.set(true)),
            null
        );

        assertThat(succeeded.get(), is(true));
    }

    public void testMintAccessSuccessShouldNotReleaseBorrowedResponse() {
        TestEsqlQueryResponse response = refCountedEmptyMintProbeResponse();
        ValidatorHarness harness = new ValidatorHarness(response);

        assertThat(response.hasReferences(), is(true));
        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            ESQL_QUERY,
            null,
            ActionTestUtils.assertNoFailureListener(ignored -> succeeded.set(true)),
            null
        );

        assertThat(succeeded.get(), is(true));
        assertThat(response.hasReferences(), is(false));
    }

    public void testMintAccessWithExplicitMissingProjectShouldFailBeforeMint() {
        NoMatchingProjectException noMatchingProject = NoMatchingProjectTestMessages.explicitMissingQualifiedProject();
        ValidatorHarness harness = new ValidatorHarness(noMatchingProject);

        AtomicReference<Exception> failure = new AtomicReference<>();
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            "FROM project-a:logs | LIMIT 10",
            "_alias:*",
            ActionListener.wrap(ignored -> fail("an explicit missing project must not be deferred"), failure::set),
            null
        );

        assertThat(failure.get(), equalTo(noMatchingProject));
    }

    public void testMintAccessWithNoProjectAfterRoutingShouldDeferRegardlessOfQueryShape() {
        NoMatchingProjectException noProjectAfterRouting = NoMatchingProjectTestMessages.noProjectAfterRouting("_alias:*");
        List<String> queriesWithNonQualifierColons = List.of(
            "FROM project-a:logs-* | LIMIT 10",
            "FROM logs | WHERE message : \"error\"",
            "FROM logs | MATCH(f, \"q\", {\"fuzziness\": 1})",
            "FROM logs | LOOKUP JOIN idx ON k",
            "FROM (FROM logs) | LIMIT 10"
        );
        for (String query : queriesWithNonQualifierColons) {
            ValidatorHarness harness = new ValidatorHarness(noProjectAfterRouting);
            AtomicBoolean deferred = new AtomicBoolean(false);
            harness.validator.validateAccessForMint(
                null,
                Collections.emptyMap(),
                query,
                "_alias:*",
                ActionTestUtils.assertNoFailureListener(ignored -> deferred.set(true)),
                null
            );
            assertThat("query [" + query + "]", deferred.get(), is(true));
        }
    }

    public void testMintAccessWithQualifiedSourceAndIndexNotFoundShouldDefer() {
        ValidatorHarness harness = new ValidatorHarness(new IndexNotFoundException("project-a:logs"));

        AtomicBoolean succeeded = new AtomicBoolean(false);
        harness.validator.validateAccessForMint(
            null,
            Collections.emptyMap(),
            "FROM project-a:logs | LIMIT 10",
            "_alias:*",
            ActionTestUtils.assertNoFailureListener(ignored -> succeeded.set(true)),
            null
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
            }),
            null
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
            }),
            null
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
            ActionListener.wrap(ignored -> fail("expected failure"), failure::set),
            null
        );

        assertThat(failure.get(), equalTo(securityFailure));
    }

    public void testLookupJoinUnknownIndexShouldDeferExistenceFailure() {
        VerificationException lookupMissing = new VerificationException("Unknown index [fake-lookup]");
        assertTrue(EsqlDatafeedQueryValidator.isDeferredExistenceFailure(lookupMissing));
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

    private static void checkColumns(List<ColumnInfo> columns, String timeField, String requiredSummaryCountField, String datafeedId) {
        Map<String, String> types = new LinkedHashMap<>();
        columns.forEach(column -> types.put(column.name(), column.outputType()));
        EsqlDatafeedQueryValidator.checkRequiredColumns(types, timeField, requiredSummaryCountField, datafeedId);
    }

    public void testAllRequiredColumnsPresentShouldSucceedCheck() {
        List<ColumnInfo> columns = List.of(
            mockColumn(TIME_FIELD, "date"),
            mockColumn(SUMMARY_COUNT_FIELD, "long"),
            mockColumn("other", "keyword")
        );
        checkColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null);
    }

    public void testIntegerSummaryCountColumnShouldPassColumnCheck() {
        checkColumns(
            List.of(mockColumn(TIME_FIELD, "date"), mockColumn(SUMMARY_COUNT_FIELD, "integer")),
            TIME_FIELD,
            SUMMARY_COUNT_FIELD,
            null
        );
    }

    public void testDoubleSummaryCountColumnShouldFailColumnCheck() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> checkColumns(
                List.of(mockColumn(TIME_FIELD, "date"), mockColumn(SUMMARY_COUNT_FIELD, "double")),
                TIME_FIELD,
                SUMMARY_COUNT_FIELD,
                null
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(Messages.getMessage(Messages.DATAFEED_ESQL_DELAYED_DATA_SUMMARY_COUNT_NOT_INTEGER, SUMMARY_COUNT_FIELD, "double"))
        );
    }

    public void testKeywordSummaryCountColumnShouldFailColumnCheck() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> checkColumns(
                List.of(mockColumn(TIME_FIELD, "date"), mockColumn(SUMMARY_COUNT_FIELD, "keyword")),
                TIME_FIELD,
                SUMMARY_COUNT_FIELD,
                null
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(Messages.getMessage(Messages.DATAFEED_ESQL_DELAYED_DATA_SUMMARY_COUNT_NOT_INTEGER, SUMMARY_COUNT_FIELD, "keyword"))
        );
    }

    public void testNoSummaryCountFieldRequiredShouldSucceedColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        checkColumns(columns, TIME_FIELD, null, null);
    }

    public void testMissingTimeFieldShouldThrowFromColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> checkColumns(columns, TIME_FIELD, null, null));
        assertThat(e.getMessage(), containsString("final ES|QL output"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testMissingTimeFieldWithDatafeedIdShouldNameDatafeedInColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn("other_field", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> checkColumns(columns, TIME_FIELD, null, "esql-datafeed")
        );
        assertThat(e.getMessage(), containsString("ES|QL datafeed [esql-datafeed] query"));
        assertThat(e.getMessage(), containsString("data_description.time_field [" + TIME_FIELD + "]"));
    }

    public void testMissingSummaryCountFieldShouldThrowFromColumnCheck() {
        List<ColumnInfo> columns = List.of(mockColumn(TIME_FIELD, "date"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> checkColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null)
        );
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(e.getMessage(), containsString("numeric count column"));
    }

    public void testBothRequiredColumnsMissingShouldListBothInError() {
        List<ColumnInfo> columns = List.of(mockColumn("unrelated", "keyword"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> checkColumns(columns, TIME_FIELD, SUMMARY_COUNT_FIELD, null)
        );
        assertThat(e.getMessage(), containsString(TIME_FIELD));
        assertThat(e.getMessage(), containsString(SUMMARY_COUNT_FIELD));
        assertThat(e.getMessage(), containsString("final ES|QL output"));
        assertThat(e.getMessage(), containsString("numeric count column"));
    }

    public void testEmptyColumnsShouldThrowFromColumnCheck() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> checkColumns(List.of(), TIME_FIELD, null, null));
        assertThat(e.getMessage(), containsString(TIME_FIELD));
    }

    public void testRequiredSummaryCountFieldShouldStayNullUnlessDelayedCheckIsEnabled() {
        Job jobWithField = buildJob("job-1", SUMMARY_COUNT_FIELD);
        assertThat(
            EsqlDatafeedQueryValidator.requiredSummaryCountField(
                buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig()),
                jobWithField
            ),
            equalTo(SUMMARY_COUNT_FIELD)
        );
        assertThat(
            EsqlDatafeedQueryValidator.requiredSummaryCountField(
                buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.disabledDelayedDataCheckConfig()),
                jobWithField
            ),
            nullValue()
        );
        Job jobWithoutField = buildJob("job-1", null);
        assertThat(
            EsqlDatafeedQueryValidator.requiredSummaryCountField(
                buildDatafeed("datafeed-1", "job-1", DelayedDataCheckConfig.defaultDelayedDataCheckConfig()),
                jobWithoutField
            ),
            nullValue()
        );
        assertThat(
            EsqlDatafeedQueryValidator.requiredSummaryCountField(
                buildDatafeedWithNullDelayedDataCheckConfig("datafeed-1", "job-1"),
                jobWithField
            ),
            nullValue()
        );
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

    private TestEsqlQueryResponse buildResponse(List<ColumnInfo> columns) {
        return new TestEsqlQueryResponse(mockEsqlResponse(columns));
    }

    public void testProbeSuccessShouldNotReleaseBorrowedResponse() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped"));

        assertThat(harness.response.hasReferences(), is(true));
        assertValidationSucceeds(harness, validation());
        assertThat(harness.response.hasReferences(), is(false));
    }

    public void testProbeFailureShouldNotReleaseBorrowedResponse() {
        ValidatorHarness harness = harness(row("other_field", "keyword", null, null, "mapped"));

        assertThat(harness.response.hasReferences(), is(true));
        assertValidationFails(harness, validation());
        assertThat(harness.response.hasReferences(), is(false));
    }

    public void testProbeListenerThrowsShouldNotReleaseBorrowedResponse() {
        ValidatorHarness harness = harness(row("@timestamp", "date", "time", "@timestamp", "mapped"));

        assertThat(harness.response.hasReferences(), is(true));
        validation().run(harness, ActionListener.wrap(ok -> { throw new IllegalStateException("listener failure"); }, e -> {}));
        assertThat(harness.response.hasReferences(), is(false));
    }

    private static final List<String> DESCRIBE_COLUMNS = List.of(
        "name",
        "type",
        "role",
        "field",
        "origin",
        "metric_type",
        "unit",
        "interval",
        "time_zone",
        "description"
    );

    /** A DESCRIBE row: name, type, role, field, origin and optionally interval. */
    private static List<Object> row(String name, String type, String role, String field, String origin, String... interval) {
        return Arrays.asList(name, type, role, field, origin, null, null, interval.length == 0 ? null : interval[0], null, null);
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private ValidatorHarness harness(List<Object>... rows) {
        return new ValidatorHarness(Arrays.asList(rows));
    }

    private ColumnInfo mockColumn(String name) {
        return mockColumn(name, "keyword");
    }

    @SuppressWarnings("unchecked")
    private EsqlResponse describeResponse(List<EsqlClusterInfo> clusters, List<String> header, List<List<Object>> rows) {
        EsqlResponse response = mock(EsqlResponse.class);
        doReturn(header.stream().map(this::mockColumn).toList()).when(response).columns();
        when(response.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) rows);
        doReturn(clusters).when(response).clusters();
        return response;
    }

    private static Validation validation() {
        return new Validation();
    }

    private void assertValidationSucceeds(ValidatorHarness harness, Validation validation) {
        AtomicBoolean succeeded = new AtomicBoolean(false);
        AtomicReference<Exception> failure = new AtomicReference<>();
        validation.run(harness, ActionListener.wrap(ok -> succeeded.set(true), failure::set));
        assertThat(failure.get(), nullValue());
        assertThat(succeeded.get(), is(true));
    }

    private Exception assertValidationFails(ValidatorHarness harness, Validation validation) {
        AtomicReference<Exception> failure = new AtomicReference<>();
        validation.run(harness, ActionListener.wrap(ok -> fail("expected validation failure"), failure::set));
        assertThat(failure.get(), notNullValue());
        return failure.get();
    }

    /** Arguments of one validateQuery call; defaults describe a datafeed whose time column and source_time_field are @timestamp. */
    private static final class Validation {
        private String query = ESQL_QUERY;
        private String projectRouting;
        private String timeField = TIME_FIELD;
        private String sourceTimeField = TIME_FIELD;
        private String summaryCountField;
        private boolean crossProjectEnabled;

        Validation query(String query) {
            this.query = query;
            return this;
        }

        Validation projectRouting(String projectRouting) {
            this.projectRouting = projectRouting;
            return this;
        }

        Validation timeField(String timeField) {
            this.timeField = timeField;
            return this;
        }

        Validation sourceTimeField(String sourceTimeField) {
            this.sourceTimeField = sourceTimeField;
            return this;
        }

        Validation summaryCountField(String summaryCountField) {
            this.summaryCountField = summaryCountField;
            return this;
        }

        Validation crossProjectEnabled(boolean crossProjectEnabled) {
            this.crossProjectEnabled = crossProjectEnabled;
            return this;
        }

        void run(ValidatorHarness harness, ActionListener<Boolean> listener) {
            harness.validator.validateQuery(
                null,
                Collections.emptyMap(),
                query,
                projectRouting,
                timeField,
                sourceTimeField,
                summaryCountField,
                crossProjectEnabled,
                listener,
                null
            );
        }
    }

    private final class ValidatorHarness {
        final EsqlDatafeedQueryValidator validator;
        TestEsqlQueryResponse response;
        String capturedQuery;
        String capturedRouting;
        List<EsqlQueryRequestBuilder.EsqlQueryParam> capturedParams;
        ExecutionMetadata capturedMetadata;
        private final Exception failure;
        private List<List<Object>> describeRows;

        ValidatorHarness(TestEsqlQueryResponse response) {
            this.response = response;
            this.failure = null;
            this.validator = newValidator();
        }

        ValidatorHarness(Exception failure) {
            this.failure = failure;
            this.validator = newValidator();
        }

        ValidatorHarness(List<List<Object>> describeRows) {
            this.describeRows = describeRows;
            this.failure = null;
            this.response = new TestEsqlQueryResponse(describeResponse(List.of(), DESCRIBE_COLUMNS, describeRows));
            this.validator = newValidator();
        }

        ValidatorHarness withClusters(EsqlClusterInfo... clusters) {
            response = new TestEsqlQueryResponse(describeResponse(List.of(clusters), DESCRIBE_COLUMNS, describeRows));
            return this;
        }

        private EsqlDatafeedQueryValidator newValidator() {
            return new EsqlDatafeedQueryValidator((client, query, filter, headers, projectRouting, params, metadata, listener) -> {
                capturedQuery = query;
                capturedRouting = projectRouting;
                capturedParams = params;
                capturedMetadata = metadata;
                if (failure != null) {
                    listener.onFailure(failure);
                } else {
                    @SuppressWarnings("unchecked")
                    ActionListener<RefCounted> refCountedListener = (ActionListener<RefCounted>) (ActionListener<?>) listener;
                    ActionListener.respondAndRelease(refCountedListener, response);
                }
            });
        }
    }

    private static TestEsqlQueryResponse refCountedEmptyMintProbeResponse() {
        return new TestEsqlQueryResponse(mock(EsqlResponse.class));
    }

    private static final class TestEsqlQueryResponse extends EsqlQueryResponse implements RefCounted {

        private final AbstractRefCounted counted = AbstractRefCounted.of(() -> {});
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

        @Override
        public void incRef() {
            counted.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return counted.tryIncRef();
        }

        @Override
        public void mustIncRef() {
            counted.mustIncRef();
        }

        @Override
        public boolean decRef() {
            return counted.decRef();
        }

        @Override
        public boolean hasReferences() {
            return counted.hasReferences();
        }

        @Override
        public void close() {
            super.close();
            decRef();
        }
    }
}
