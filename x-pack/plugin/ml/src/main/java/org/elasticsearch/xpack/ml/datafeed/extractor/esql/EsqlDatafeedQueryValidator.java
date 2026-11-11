/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

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

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;

/**
 * Includes helper functions for validating the ESQL query provided to a datafeed.
 */
public class EsqlDatafeedQueryValidator {

    private final EsqlQueryRunner queryRunner;

    public EsqlDatafeedQueryValidator() {
        this(EsqlQueryRunner.DEFAULT);
    }

    EsqlDatafeedQueryValidator(EsqlQueryRunner queryRunner) {
        this.queryRunner = queryRunner;
    }

    private static final String LIMIT_ZERO = " | LIMIT 0";
    private static final String KEEP_SOURCE_TIME_FIELD_LIMIT_ZERO = " | KEEP ??sourceTimeField | LIMIT 0";

    // MUST FIX before production: matching the ES|QL error text is brittle and breaks if the message changes; ES|QL needs to
    // expose a typed missing-index signal. See https://github.com/elastic/elasticsearch/issues/161207
    /** ES|QL reports a missing index as 400 with this fragment; ML cannot depend on the ES|QL verifier type. */
    private static final String ESQL_UNKNOWN_INDEX_MESSAGE = "Unknown index [";

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
        warnForConflictingOuterClauses(datafeedId, esqlQuery, timeField);
        String limitZeroQuery = EsqlQueryClauseScanner.appendGeneratedPipeline(esqlQuery, LIMIT_ZERO);

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            try {
                checkRequiredColumns(response.response().columns(), timeField, summaryCountField, datafeedId);
                listener.onResponse(Boolean.TRUE);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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
     * Probes {@code KEEP ??sourceTimeField | LIMIT 0} on the leading FROM/TS command so source_time_field
     * resolves like {@link EsqlDataExtractor}; tolerates deferred existence like {@link #validateQuery}.
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
        String sourceCommand = extractLeadingSourceCommand(esqlQuery);
        if (sourceCommand == null) {
            // Cannot build a source probe without a leading FROM/TS command.
            listener.onResponse(Boolean.TRUE);
            return;
        }
        String probeQuery = EsqlQueryClauseScanner.appendGeneratedPipeline(sourceCommand, KEEP_SOURCE_TIME_FIELD_LIMIT_ZERO);
        List<EsqlQueryParam> params = List.of(new EsqlQueryParam("sourceTimeField", sourceTimeField, IDENTIFIER));

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            try {
                checkSourceTimeFieldType(response.response().columns(), sourceTimeField, datafeedId);
                listener.onResponse(Boolean.TRUE);
            } catch (Exception e) {
                listener.onFailure(e);
            }
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

    /** Leading FROM/TS segment for source probes; {@code null} if the query does not start with FROM or TS. */
    static String extractLeadingSourceCommand(String esqlQuery) {
        String leading = EsqlQueryClauseScanner.extractLeadingCommand(esqlQuery).strip();
        int commandStart = EsqlQueryClauseScanner.skipLeadingWhitespaceAndComments(leading);
        return isSourceCommand(leading, commandStart) ? leading : null;
    }

    /**
     * Rejects remote-cluster sources in the leading FROM/TS command until ES|QL datafeed remote checks exist.
     * No-op when {@code crossProjectEnabled} (CPS {@code project:index} is not a remote cluster).
     */
    public static void rejectRemoteClusterSources(@Nullable String esqlQuery, boolean crossProjectEnabled) {
        if (esqlQuery == null || crossProjectEnabled) {
            return;
        }
        String sourceCommand = extractLeadingSourceCommand(esqlQuery);
        if (sourceCommand == null) {
            return;
        }
        for (String indexExpression : sourceIndexExpressions(sourceCommand)) {
            String clusterAlias = remoteClusterAlias(indexExpression);
            if (clusterAlias != null) {
                throw ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.DATAFEED_ESQL_REMOTE_CLUSTER_SOURCE_NOT_SUPPORTED, indexExpression, clusterAlias)
                );
            }
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

    // MUST FIX before production: hand-written parsing of the source command (misses e.g. FROM (FROM remote:logs)); use EsqlParser.
    // See https://github.com/elastic/elasticsearch/issues/161207
    /** Index expressions of a leading {@code FROM}/{@code TS} command, in order. */
    private static List<String> sourceIndexExpressions(String sourceCommand) {
        int index = EsqlQueryClauseScanner.skipLeadingWhitespaceAndComments(sourceCommand);
        index += matchesCommandKeyword(sourceCommand, index, "FROM") ? "FROM".length() : "TS".length();
        List<String> expressions = new ArrayList<>();
        while (true) {
            index = EsqlQueryClauseScanner.skipWhitespaceAndComments(sourceCommand, index);
            if (index >= sourceCommand.length()) {
                break;
            }
            int tokenEnd;
            String expression;
            if (sourceCommand.charAt(index) == '"') {
                tokenEnd = EsqlQueryClauseScanner.skipQuotedString(sourceCommand, index);
                int quoteLength = sourceCommand.startsWith("\"\"\"", index) ? 3 : 1;
                int contentEnd = Math.max(index + quoteLength, tokenEnd - quoteLength);
                expression = sourceCommand.substring(index + quoteLength, contentEnd);
                // ES|QL rejects a quoted cluster part ("east":logs), but a cluster prefix outside the quotes is still remote.
                if (tokenEnd < sourceCommand.length()
                    && sourceCommand.charAt(tokenEnd) == RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR
                    && sourceCommand.startsWith("::", tokenEnd) == false) {
                    expression = expression + RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR;
                }
            } else {
                tokenEnd = index;
                while (tokenEnd < sourceCommand.length() && isUnquotedSourceCharacter(sourceCommand, tokenEnd)) {
                    tokenEnd++;
                }
                expression = sourceCommand.substring(index, tokenEnd);
            }
            if (tokenEnd == index) {
                break;
            }
            expressions.add(expression);
            index = EsqlQueryClauseScanner.skipWhitespaceAndComments(sourceCommand, tokenEnd);
            if (index < sourceCommand.length() && sourceCommand.charAt(index) == ',') {
                index++;
            } else {
                break;
            }
        }
        return expressions;
    }

    private static boolean isUnquotedSourceCharacter(String text, int index) {
        char character = text.charAt(index);
        return Character.isWhitespace(character) == false
            && character != ','
            && text.startsWith("//", index) == false
            && text.startsWith("/*", index) == false;
    }

    /** Whether {@code text} has a FROM or TS keyword at {@code index} (TS is probed like FROM at runtime). */
    private static boolean isSourceCommand(String text, int index) {
        return matchesCommandKeyword(text, index, "FROM") || matchesCommandKeyword(text, index, "TS");
    }

    private static boolean matchesCommandKeyword(String text, int index, String keyword) {
        int end = index + keyword.length();
        return end < text.length()
            && text.regionMatches(true, index, keyword, 0, keyword.length())
            && Character.isWhitespace(text.charAt(end));
    }

    private static void checkSourceTimeFieldType(List<? extends ColumnInfo> columns, String sourceTimeField, @Nullable String datafeedId) {
        // KEEP probe returns the one requested column or fails before a response (validateSourceTimeField onFailure).
        String outputType = columns.isEmpty() ? null : columns.get(0).outputType();
        if (EsqlDatafeedColumnValues.isDateColumnType(outputType) == false) {
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
    static void warnForConflictingOuterClauses(@Nullable String datafeedId, String esqlQuery, String timeField) {
        EsqlQueryClauseScanner.ScanResult scan = EsqlQueryClauseScanner.scan(esqlQuery, timeField);
        String queryContext = esqlDatafeedSubject(datafeedId);
        if (scan.hasOuterTimeWhere()) {
            HeaderWarning.addWarning(Messages.getMessage(Messages.DATAFEED_ESQL_OUTER_TIME_WHERE_WARNING, queryContext, timeField));
        }
        if (scan.hasOuterTimeSort()) {
            HeaderWarning.addWarning(Messages.getMessage(Messages.DATAFEED_ESQL_OUTER_TIME_SORT_WARNING, queryContext, timeField));
        }
        if (scan.hasOuterLimit()) {
            HeaderWarning.addWarning(Messages.getMessage(Messages.DATAFEED_ESQL_OUTER_LIMIT_WARNING, queryContext));
        }
    }

    /** {@code LIMIT 0} access probe before CPS credential mint; column checks are {@link #validateQuery}. */
    public void validateAccessForMint(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        ActionListener<Void> listener
    ) {
        String limitZeroQuery = EsqlQueryClauseScanner.appendGeneratedPipeline(esqlQuery, LIMIT_ZERO);

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> listener.onResponse(null), e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (isDeferredExistenceFailure(cause)) {
                // Source may not exist yet; defer to runtime (classic SearchRequest probe behaviour).
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, limitZeroQuery, headers, projectRouting, List.of(), responseListener);
    }

    static void validateEmittedTimesInSourceWindow(
        List<? extends ColumnInfo> columns,
        Iterable<? extends Iterable<Object>> rows,
        String jobId,
        String emittedTimeField,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        int emittedTimeColumnIndex = indexOfColumn(columns, emittedTimeField);
        boolean emittedTimeIsDate = EsqlDatafeedColumnValues.isDateColumnType(columns.get(emittedTimeColumnIndex).outputType());
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
        for (ColumnInfo column : columns) {
            String name = column.name();
            if (timeField.equals(name)) {
                foundTimeField = true;
            }
            if (requiredSummaryCountField != null && requiredSummaryCountField.equals(name)) {
                foundSummaryCountField = true;
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
    }

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
