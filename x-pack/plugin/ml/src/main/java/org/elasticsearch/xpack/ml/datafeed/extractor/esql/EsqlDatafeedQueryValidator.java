/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryAnalyzer;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryStructure;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;

/**
 * Includes helper functions for validating the ESQL query provided to a datafeed.
 */
public class EsqlDatafeedQueryValidator {

    private static final Logger logger = LogManager.getLogger(EsqlDatafeedQueryValidator.class);

    private static final String PROJECT_ROUTING_SETTING = "project_routing";
    private static final String TIME_ZONE_SETTING = "time_zone";

    private final EsqlQueryAnalyzer analyzer;
    private final EsqlQueryRunner queryRunner;

    public EsqlDatafeedQueryValidator(EsqlQueryAnalyzer analyzer) {
        this(analyzer, EsqlQueryRunner.DEFAULT);
    }

    EsqlDatafeedQueryValidator(EsqlQueryAnalyzer analyzer, EsqlQueryRunner queryRunner) {
        this.analyzer = Objects.requireNonNull(analyzer);
        this.queryRunner = Objects.requireNonNull(queryRunner);
    }

    // MUST FIX before production: matching the ES|QL error text is brittle and breaks if the message changes; ES|QL needs to
    // expose a typed missing-index signal. See https://github.com/elastic/elasticsearch/issues/161207
    /** ES|QL reports a missing index as 400 with this fragment; ML cannot depend on the ES|QL verifier type. */
    private static final String ESQL_UNKNOWN_INDEX_MESSAGE = "Unknown index [";

    // MUST FIX before production: message match on NoMatchingProjectException; add a typed no-project signal (bead
    // elastic-workspace-u66g). See https://github.com/elastic/elasticsearch/issues/161207
    /** Thrown when project routing matches no project at all; a linked project may appear later. */
    static final String NO_PROJECT_AFTER_ROUTING_MESSAGE = "no matching project after applying project routing";

    static boolean isNoProjectAfterRouting(Throwable cause) {
        return cause instanceof NoMatchingProjectException noMatchingProject
            && Strings.isEmpty(noMatchingProject.getMessage()) == false
            && noMatchingProject.getMessage().contains(NO_PROJECT_AFTER_ROUTING_MESSAGE);
    }

    /** Whether a probe failure means the source does not exist yet (index or CPS project may appear later). */
    static boolean isDeferredExistenceFailure(Throwable cause) {
        if (cause instanceof NoMatchingProjectException || cause instanceof IndexNotFoundException) {
            return true;
        }
        return cause instanceof ElasticsearchException esException
            && esException.status() == RestStatus.BAD_REQUEST
            && esException.getDetailedMessage().contains(ESQL_UNKNOWN_INDEX_MESSAGE);
    }

    /**
     * Returns the summary count field name that the ESQL query must output, or {@code null} if it is
     * not required. The field is only required when the job configures a {@code summary_count_field_name}
     * and the datafeed's delayed data check is enabled.
     */
    @Nullable
    public static String requiredSummaryCountField(DatafeedConfig datafeed, Job job) {
        String summaryCountField = job.getAnalysisConfig().getSummaryCountFieldName();
        DelayedDataCheckConfig delayedDataCheckConfig = datafeed.getDelayedDataCheckConfig();
        boolean delayedDataCheckEnabled = delayedDataCheckConfig != null && delayedDataCheckConfig.isEnabled();
        return (Strings.hasText(summaryCountField) && delayedDataCheckEnabled) ? summaryCountField : null;
    }

    /** Runs {@code esqlQuery | LIMIT 0} and checks required output columns; tolerates deferred source existence. */
    public void validateQuery(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        @Nullable String summaryCountField,
        ActionListener<Boolean> listener
    ) {
        validateQuery(client, headers, esqlQuery, projectRouting, timeField, summaryCountField, listener, null);
    }

    public void validateQuery(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        @Nullable String summaryCountField,
        ActionListener<Boolean> listener,
        @Nullable String datafeedId
    ) {
        EsqlDatafeedQuery query;
        try {
            query = EsqlDatafeedQuery.parse(analyzer, esqlQuery);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        warnForConflictingOuterClauses(datafeedId, query, timeField, summaryCountField != null);
        String limitZeroQuery = EsqlDatafeedQuery.limitZeroQuery(esqlQuery);

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            try {
                checkRequiredColumns(response.response().columns(), timeField, summaryCountField, datafeedId);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(Boolean.TRUE);
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (isDeferredExistenceFailure(cause)) {
                // Source may not exist yet; skip column checks (same as DSL datafeed PUT).
                listener.onResponse(Boolean.TRUE);
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, limitZeroQuery, headers, projectRouting, List.of(), responseListener);
    }

    /**
     * Probes {@code KEEP ??sourceTimeField | LIMIT 0} on the source command so source_time_field resolves like
     * {@link EsqlDataExtractor}; tolerates deferred existence like {@link #validateQuery}.
     */
    public void validateSourceTimeField(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String sourceTimeField,
        ActionListener<Boolean> listener,
        @Nullable String datafeedId
    ) {
        String probeQuery;
        try {
            probeQuery = EsqlDatafeedQuery.parse(analyzer, esqlQuery).sourceTimeFieldProbeQuery();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        List<EsqlQueryParam> params = List.of(new EsqlQueryParam("sourceTimeField", sourceTimeField, IDENTIFIER));

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            try {
                checkSourceTimeFieldType(response.response().columns(), sourceTimeField, datafeedId);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(Boolean.TRUE);
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (isDeferredExistenceFailure(cause)) {
                // Source may not exist yet; skip column checks (same as DSL datafeed PUT).
                listener.onResponse(Boolean.TRUE);
            } else if (cause instanceof ElasticsearchException esException && esException.status() == RestStatus.BAD_REQUEST) {
                listener.onFailure(sourceTimeFieldUnresolvedException(sourceTimeField, datafeedId, cause));
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, probeQuery, headers, projectRouting, params, responseListener);
    }

    /**
     * Rejects query shapes ES|QL datafeeds do not support: no FROM/TS source, {@code SET project_routing}, a non-UTC
     * {@code SET time_zone}, and remote-cluster sources unless {@code crossProjectEnabled} ({@code project:index} is not remote).
     */
    public void validateQueryStructure(@Nullable String esqlQuery, boolean crossProjectEnabled) {
        if (esqlQuery == null) {
            return;
        }
        EsqlQueryStructure structure = EsqlDatafeedQuery.parse(analyzer, esqlQuery).structure();
        if (structure.settings().containsKey(PROJECT_ROUTING_SETTING)) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_ESQL_PROJECT_ROUTING_SETTING_NOT_SUPPORTED));
        }
        String timeZone = structure.settings().get(TIME_ZONE_SETTING);
        if (timeZone != null && isUtc(timeZone) == false) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_ESQL_TIME_ZONE_SETTING_NOT_UTC, timeZone));
        }
        if (crossProjectEnabled) {
            return;
        }
        // The parser already rejects a remote LOOKUP JOIN index.
        for (String indexExpression : structure.sourcePatterns()) {
            String clusterAlias = remoteClusterAlias(indexExpression);
            if (clusterAlias != null) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.DATAFEED_ESQL_REMOTE_CLUSTER_SOURCE_NOT_SUPPORTED, indexExpression, clusterAlias)
                );
            }
        }
    }

    private static boolean isUtc(String timeZone) {
        try {
            return ZoneId.of(timeZone).normalized().equals(ZoneOffset.UTC);
        } catch (DateTimeException e) {
            // ES|QL rejects an unknown zone when the query runs; here it is simply not UTC.
            return false;
        }
    }

    /** Remote cluster alias for one index expression, or {@code null} for local/CPS {@code _origin} sources. */
    @Nullable
    private static String remoteClusterAlias(String indexExpression) {
        String expression = indexExpression.startsWith("-") ? indexExpression.substring(1) : indexExpression;
        if (RemoteClusterAware.isRemoteIndexName(expression) == false) {
            return null;
        }
        String clusterAlias = RemoteClusterAware.splitIndexName(expression).clusterAlias();
        return "_origin".equals(clusterAlias) ? null : clusterAlias;
    }

    private void checkSourceTimeFieldType(List<? extends ColumnInfo> columns, String sourceTimeField, @Nullable String datafeedId) {
        // KEEP probe returns the one requested column or fails before a response (validateSourceTimeField onFailure).
        String outputType = columns.isEmpty() ? null : columns.get(0).outputType();
        if (outputType == null || analyzer.isDateType(outputType) == false) {
            throw new IllegalArgumentException(
                Messages.getMessage(
                    Messages.DATAFEED_ESQL_SOURCE_TIME_FIELD_NOT_DATE,
                    esqlDatafeedSubject(datafeedId),
                    sourceTimeField,
                    outputType
                )
            );
        }
    }

    private static IllegalArgumentException sourceTimeFieldUnresolvedException(
        String sourceTimeField,
        @Nullable String datafeedId,
        Throwable cause
    ) {
        String causeMessage = cause == null ? Messages.getMessage(Messages.DATAFEED_ESQL_PROBE_UNKNOWN_ERROR) : cause.getMessage();
        String message = Messages.getMessage(
            Messages.DATAFEED_ESQL_SOURCE_TIME_FIELD_UNRESOLVED,
            esqlDatafeedSubject(datafeedId),
            sourceTimeField,
            causeMessage
        );
        return new IllegalArgumentException(message, cause);
    }

    /**
     * Warns about outer user pipeline clauses that conflict with the time range, order, and row cap owned by ML.
     */
    static void warnForConflictingOuterClauses(
        @Nullable String datafeedId,
        EsqlDatafeedQuery query,
        String timeField,
        boolean delayedDataCheckEnabled
    ) {
        String queryContext = esqlDatafeedSubject(datafeedId);
        if (query.hasOuterTimeFilter(timeField)) {
            HeaderWarning.addWarning(Messages.getMessage(Messages.DATAFEED_ESQL_OUTER_TIME_WHERE_WARNING, queryContext, timeField));
        }
        if (query.hasOuterTimeSort(timeField)) {
            HeaderWarning.addWarning(Messages.getMessage(Messages.DATAFEED_ESQL_OUTER_TIME_SORT_WARNING, queryContext, timeField));
        }
        // A LIMIT before the first STATS does not cap the output, so ML still injects its row cap.
        boolean outerLimitCapsOutput = query.hasOuterLimit() && query.injectsRowLimit() == false;
        if (outerLimitCapsOutput) {
            addLimitWarning(Messages.getMessage(Messages.DATAFEED_ESQL_OUTER_LIMIT_WARNING, queryContext), delayedDataCheckEnabled);
        }
        if (query.hasLimitBeforeAggregation()) {
            String warning = Messages.getMessage(Messages.DATAFEED_ESQL_LIMIT_BEFORE_STATS_WARNING, queryContext);
            addLimitWarning(warning, delayedDataCheckEnabled && outerLimitCapsOutput == false);
        }
    }

    private static void addLimitWarning(String warning, boolean withDelayedDataNote) {
        HeaderWarning.addWarning(withDelayedDataNote ? warning + " " + Messages.DATAFEED_ESQL_OUTER_LIMIT_DELAYED_DATA_WARNING : warning);
    }

    /**
     * {@code LIMIT 0} access probe before CPS credential mint. Defers when routing matched no project or the index is not
     * known yet; fails on explicit project resolution errors and all other probe failures.
     */
    public void validateAccessForMint(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        ActionListener<Void> listener,
        @Nullable String datafeedId
    ) {
        String limitZeroQuery = EsqlDatafeedQuery.limitZeroQuery(esqlQuery);

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> listener.onResponse(null), e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof NoMatchingProjectException) {
                if (isNoProjectAfterRouting(cause)) {
                    logger.debug(
                        () -> "[" + datafeedId + "] ES|QL validate-before-mint probe failed with [" + cause + "]. Deferring to runtime."
                    );
                    listener.onResponse(null);
                } else {
                    listener.onFailure(e);
                }
            } else if (isDeferredExistenceFailure(cause)) {
                // ES|QL reports a missing, empty-wildcard or unreadable source as unknown index; none can be told apart here.
                logger.debug(
                    () -> "[" + datafeedId + "] ES|QL validate-before-mint probe failed with [" + cause + "]. Deferring to runtime."
                );
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, limitZeroQuery, headers, projectRouting, List.of(), responseListener);
    }

    static void validateEmittedTimesInSourceWindow(
        List<? extends ColumnInfo> columns,
        EsqlQueryAnalyzer analyzer,
        Iterable<? extends Iterable<Object>> rows,
        String jobId,
        String emittedTimeField,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        int emittedTimeColumnIndex = indexOfColumn(columns, emittedTimeField);
        boolean emittedTimeIsDate = analyzer.isDateType(columns.get(emittedTimeColumnIndex).outputType());
        for (Iterable<Object> row : rows) {
            validateEmittedTimeValueInSourceWindow(
                jobId,
                emittedTimeField,
                valueAt(row, emittedTimeColumnIndex),
                emittedTimeIsDate,
                sourceWindowStart,
                sourceWindowEnd
            );
        }
    }

    /**
     * Validates and converts an emitted ES|QL timestamp that has already been materialized outside
     * the typed ES|QL response. String values are date/date_nanos representations; numeric values
     * are epoch milliseconds.
     */
    public static long validateEmittedTimeValueInSourceWindow(
        String jobId,
        String emittedTimeField,
        Object rawValue,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        return validateEmittedTimeValueInSourceWindow(
            jobId,
            emittedTimeField,
            rawValue,
            rawValue instanceof String,
            sourceWindowStart,
            sourceWindowEnd
        );
    }

    private static long validateEmittedTimeValueInSourceWindow(
        String jobId,
        String emittedTimeField,
        Object rawValue,
        boolean emittedTimeIsDate,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        var context = new EmittedTimeContext(jobId, emittedTimeField, sourceWindowStart, sourceWindowEnd);
        if (rawValue == null) {
            throw context.failure(Messages.DATAFEED_ESQL_EMITTED_TIME_NULL, null, null);
        }
        if (rawValue instanceof List<?>) {
            throw context.failure(Messages.DATAFEED_ESQL_EMITTED_TIME_MULTI_VALUED, null, null);
        }
        final long emittedTimeMillis;
        try {
            emittedTimeMillis = EsqlDatafeedColumnValues.toEpochMillis(rawValue, emittedTimeIsDate);
        } catch (IllegalArgumentException | DateTimeException e) {
            throw context.failure(Messages.DATAFEED_ESQL_EMITTED_TIME_UNSUPPORTED_TYPE, null, e);
        }
        if (emittedTimeMillis < sourceWindowStart) {
            throw context.failure(Messages.DATAFEED_ESQL_EMITTED_TIME_BEFORE_WINDOW, emittedTimeMillis, null);
        }
        if (emittedTimeMillis >= sourceWindowEnd) {
            throw context.failure(Messages.DATAFEED_ESQL_EMITTED_TIME_AT_OR_AFTER_WINDOW_END, emittedTimeMillis, null);
        }
        return emittedTimeMillis;
    }

    private record EmittedTimeContext(String jobId, String emittedTimeField, long sourceWindowStart, long sourceWindowEnd) {
        IllegalArgumentException failure(String messagePattern, @Nullable Long offendingValue, @Nullable Throwable cause) {
            return new IllegalArgumentException(
                Messages.getMessage(messagePattern, jobId, emittedTimeField, sourceWindowStart, sourceWindowEnd, offendingValue),
                cause
            );
        }
    }

    /** The ES|QL datafeed subject, with the datafeed id when known (PUT-time validation may not have one). */
    static String esqlDatafeedSubject(@Nullable String datafeedId) {
        return datafeedId == null
            ? Messages.DATAFEED_ESQL_SUBJECT
            : Messages.getMessage(Messages.DATAFEED_ESQL_SUBJECT_WITH_ID, datafeedId);
    }

    private static int indexOfColumn(List<? extends ColumnInfo> columns, String columnName) {
        for (int index = 0; index < columns.size(); index++) {
            if (columnName.equals(columns.get(index).name())) {
                return index;
            }
        }
        throw new IllegalArgumentException(Messages.getMessage(Messages.DATAFEED_ESQL_RESPONSE_MISSING_COLUMN, columnName));
    }

    private static Object valueAt(Iterable<Object> row, int columnIndex) {
        Iterator<Object> values = row.iterator();
        for (int index = 0; index < columnIndex; index++) {
            if (values.hasNext() == false) {
                return null;
            }
            values.next();
        }
        return values.hasNext() ? values.next() : null;
    }

    private static String missingTimeColumnMessage(String timeField, @Nullable String datafeedId) {
        return Messages.getMessage(Messages.DATAFEED_ESQL_MISSING_TIME_COLUMN, esqlDatafeedSubject(datafeedId), timeField);
    }

    static void checkRequiredColumns(
        List<? extends ColumnInfo> columns,
        String timeField,
        @Nullable String requiredSummaryCountField,
        @Nullable String datafeedId
    ) {
        boolean foundTimeField = false;
        boolean foundSummaryCountField = requiredSummaryCountField == null;
        String summaryCountColumnType = null;
        for (ColumnInfo column : columns) {
            String name = column.name();
            if (timeField.equals(name)) {
                foundTimeField = true;
            }
            if (requiredSummaryCountField != null && requiredSummaryCountField.equals(name)) {
                foundSummaryCountField = true;
                summaryCountColumnType = column.outputType();
            }
        }
        if (foundTimeField == false || foundSummaryCountField == false) {
            if (foundTimeField == false && foundSummaryCountField == false) {
                throw new IllegalArgumentException(
                    missingTimeColumnMessage(timeField, datafeedId)
                        + " "
                        + Messages.getMessage(Messages.DATAFEED_ESQL_DELAYED_DATA_MISSING_SUMMARY_COUNT_COLUMN, requiredSummaryCountField)
                );
            }
            if (foundTimeField == false) {
                throw new IllegalArgumentException(missingTimeColumnMessage(timeField, datafeedId));
            }
            throw new IllegalArgumentException(
                Messages.getMessage(Messages.DATAFEED_ESQL_DELAYED_DATA_MISSING_SUMMARY_COUNT_COLUMN, requiredSummaryCountField)
            );
        }
        if (requiredSummaryCountField != null && isIntegerCountColumnType(summaryCountColumnType) == false) {
            throw new IllegalArgumentException(
                Messages.getMessage(
                    Messages.DATAFEED_ESQL_DELAYED_DATA_SUMMARY_COUNT_NOT_INTEGER,
                    requiredSummaryCountField,
                    summaryCountColumnType
                )
            );
        }
    }

    private static boolean isIntegerCountColumnType(@Nullable String outputType) {
        return "long".equals(outputType) || "integer".equals(outputType);
    }

    /**
     * The listener borrows the response; it is released after {@code onResponse} returns, so call {@code incRef()} to keep it.
     */
    private void executeEsqlQueryAsync(
        Client client,
        String query,
        Map<String, String> headers,
        @Nullable String projectRouting,
        List<EsqlQueryParam> params,
        ActionListener<EsqlQueryResponse> listener
    ) {
        queryRunner.executeAsync(client, query, null, headers, projectRouting, params, listener);
    }
}
