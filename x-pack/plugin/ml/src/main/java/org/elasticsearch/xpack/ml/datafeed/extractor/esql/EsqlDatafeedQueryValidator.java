/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
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
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.ExecutionMetadata;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;

import java.time.DateTimeException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Includes helper functions for validating the ESQL query provided to a datafeed.
 */
public class EsqlDatafeedQueryValidator {

    private static final Logger logger = LogManager.getLogger(EsqlDatafeedQueryValidator.class);

    private final EsqlQueryRunner queryRunner;
    /** Whether the ES|QL parser accepts DESCRIBE, which ES|QL enables only in snapshot builds for now. */
    private final BooleanSupplier describeAvailable;

    public EsqlDatafeedQueryValidator() {
        this(EsqlQueryRunner.DEFAULT, () -> Build.current().isSnapshot());
    }

    EsqlDatafeedQueryValidator(EsqlQueryRunner queryRunner, BooleanSupplier describeAvailable) {
        this.queryRunner = queryRunner;
        this.describeAvailable = describeAvailable;
    }

    private static final String LIMIT_ZERO = " | LIMIT 0";
    // MUST FIX before production: DESCRIBE is in the ES|QL dev grammar (snapshot builds only) until
    // https://github.com/elastic/elasticsearch/pull/166439 releases it. Until then validateQuery rejects ES|QL datafeeds in release
    // builds with a clear error; remove that guard once DESCRIBE is released.
    private static final String DESCRIBE = "\n| DESCRIBE";
    /** Under cross-project search {@code _origin} names the local project, so it is not a remote. */
    private static final String ORIGIN_PROJECT_ALIAS = "_origin";

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

    /**
     * Runs {@code esqlQuery | DESCRIBE} filtered to the required columns, with {@code _clusters} metadata, and checks:
     * no remote clusters (unless cross-project), that the time column is a date, and the summary count column.
     * Tolerates deferred source existence.
     */
    public void validateQuery(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        @Nullable String summaryCountField,
        boolean crossProjectEnabled,
        ActionListener<Boolean> listener,
        @Nullable String datafeedId
    ) {
        if (describeAvailable.getAsBoolean() == false) {
            listener.onFailure(
                ExceptionsHelper.badRequestException(
                    Messages.getMessage(Messages.DATAFEED_ESQL_DESCRIBE_UNAVAILABLE, esqlDatafeedSubject(datafeedId))
                )
            );
            return;
        }
        warnOnOuterLimit(esqlQuery, datafeedId);
        // Filter and LIMIT so the default result row cap cannot hide a required column when the query has many columns.
        // The names are inlined as string literals, not bound as params: params apply to the whole request, user query included.
        String describeSuffix = summaryCountField != null
            ? DESCRIBE + "\n| WHERE name IN (" + esqlString(timeField) + ", " + esqlString(summaryCountField) + ")\n| LIMIT 2"
            : DESCRIBE + "\n| WHERE name IN (" + esqlString(timeField) + ")\n| LIMIT 1";
        String describeQuery = EsqlQueryClauseScanner.appendGeneratedPipeline(esqlQuery, describeSuffix);

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            try {
                checkDescription(
                    EsqlQueryDescription.from(response.response()),
                    timeField,
                    summaryCountField,
                    crossProjectEnabled,
                    datafeedId
                );
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(Boolean.TRUE);
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (isDeferredExistenceFailure(cause)) {
                // Source may not exist yet; skip column checks (same as DSL datafeed PUT).
                // There is no response, so there is no _clusters either: a remote source whose index does not exist yet passes PUT.
                // MUST FIX before production: check remote sources at datafeed start. See
                // https://github.com/elastic/elasticsearch/issues/161207
                logger.debug(() -> "[" + datafeedId + "] ES|QL DESCRIBE probe failed with [" + cause + "]. Deferring to runtime.");
                listener.onResponse(Boolean.TRUE);
            } else {
                listener.onFailure(e);
            }
        });

        queryRunner.executeAsync(
            client,
            describeQuery,
            null,
            headers,
            projectRouting,
            List.of(),
            ExecutionMetadata.CCS_ONLY,
            responseListener
        );
    }

    /**
     * Extraction keeps a user's outer {@code LIMIT}, so it caps every chunk and the delayed data check. Warn at PUT until extraction
     * always appends its own LIMIT (https://github.com/elastic/elasticsearch/pull/167093), which removes this warning.
     */
    static void warnOnOuterLimit(String esqlQuery, @Nullable String datafeedId) {
        if (EsqlQueryClauseScanner.hasOuterLimit(esqlQuery)) {
            HeaderWarning.addWarning(Messages.getMessage(Messages.DATAFEED_ESQL_OUTER_LIMIT_WARNING, esqlDatafeedSubject(datafeedId)));
        }
    }

    /** An ES|QL double-quoted string literal for {@code value}. */
    static String esqlString(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    private static void checkDescription(
        EsqlQueryDescription description,
        String timeField,
        @Nullable String summaryCountField,
        boolean crossProjectEnabled,
        @Nullable String datafeedId
    ) {
        String subject = esqlDatafeedSubject(datafeedId);
        if (crossProjectEnabled == false) {
            for (EsqlClusterInfo cluster : description.clusters()) {
                String alias = cluster.clusterAlias();
                if (alias.equals(EsqlClusterInfo.LOCAL_CLUSTER_ALIAS) == false && alias.equals(ORIGIN_PROJECT_ALIAS) == false) {
                    String expression = cluster.indexExpression() == null
                        ? alias
                        : alias + RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR + cluster.indexExpression();
                    throw ExceptionsHelper.badRequestException(
                        Messages.getMessage(Messages.DATAFEED_ESQL_REMOTE_CLUSTER_SOURCE_NOT_SUPPORTED, subject, alias, expression)
                    );
                }
            }
        }
        Map<String, String> columnTypes = description.columnTypes();
        checkRequiredColumns(columnTypes, timeField, summaryCountField, datafeedId);
        String timeType = columnTypes.get(timeField);
        assert timeType != null : "checkRequiredColumns accepted a missing time column [" + timeField + "]";
        if (EsqlDatafeedColumnValues.isDateColumnType(timeType) == false) {
            throw new IllegalArgumentException(
                Messages.getMessage(Messages.DATAFEED_ESQL_TIME_COLUMN_NOT_DATE, subject, timeField, timeType)
            );
        }
    }

    /**
     * An ES|QL datafeed update needs the probe when its project routing or credential changed, or when it newly requires a summary
     * count column. Other updates cannot change the probe outcome. A PUT always probes and does not use this method.
     */
    public static boolean updateNeedsProbe(DatafeedConfig current, DatafeedConfig updated, Job job) {
        if (updated.getEsqlQuery() == null) {
            return false;
        }
        boolean searchScopeChanged = Objects.equals(current.getProjectRouting(), updated.getProjectRouting()) == false
            || Objects.equals(credentialId(current), credentialId(updated)) == false;
        if (searchScopeChanged) {
            return true;
        }
        String requiredBefore = requiredSummaryCountField(current, job);
        String requiredAfter = requiredSummaryCountField(updated, job);
        return requiredAfter != null && Objects.equals(requiredBefore, requiredAfter) == false;
    }

    @Nullable
    private static String credentialId(DatafeedConfig config) {
        PersistedCloudCredential credential = config.getCloudInternalCredential();
        return credential == null ? null : credential.id();
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
        String limitZeroQuery = EsqlQueryClauseScanner.appendGeneratedPipeline(esqlQuery, LIMIT_ZERO);

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

        queryRunner.executeAsync(
            client,
            limitZeroQuery,
            null,
            headers,
            projectRouting,
            List.of(),
            ExecutionMetadata.NEVER,
            responseListener
        );
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
        Map<String, String> columnTypes,
        String timeField,
        @Nullable String requiredSummaryCountField,
        @Nullable String datafeedId
    ) {
        boolean foundTimeField = columnTypes.containsKey(timeField);
        boolean foundSummaryCountField = requiredSummaryCountField == null || columnTypes.containsKey(requiredSummaryCountField);
        String summaryCountColumnType = requiredSummaryCountField == null ? null : columnTypes.get(requiredSummaryCountField);
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
}
