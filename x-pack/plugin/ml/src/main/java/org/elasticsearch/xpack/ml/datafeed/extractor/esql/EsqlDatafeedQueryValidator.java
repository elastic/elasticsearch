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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.search.crossproject.ProjectRoutingResolver;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.ExecutionMetadata;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.VALUE;

/**
 * Includes helper functions for validating the ESQL query provided to a datafeed.
 */
public class EsqlDatafeedQueryValidator {

    private static final Logger logger = LogManager.getLogger(EsqlDatafeedQueryValidator.class);

    private final EsqlQueryRunner queryRunner;

    public EsqlDatafeedQueryValidator() {
        this(EsqlQueryRunner.DEFAULT);
    }

    EsqlDatafeedQueryValidator(EsqlQueryRunner queryRunner) {
        this.queryRunner = queryRunner;
    }

    /** How {@code DESCRIBE} names the default time zone of a bucketed column. */
    private static final String UTC = "UTC";
    private static final String LIMIT_ZERO = "LIMIT 0";
    private static final String DESCRIBE = "DESCRIBE";
    /**
     * Filters and limits the {@code DESCRIBE} so the default result row cap cannot hide a required column. The param names are not
     * bound by extraction, so a user query that references them fails the probe, as it would without these params.
     */
    private static final String DESCRIBE_REQUIRED_COLUMNS = DESCRIBE
        + "\n| WHERE name IN (?describeTimeColumn, ?describeCountColumn)\n| LIMIT 2";

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
     * Runs {@code esqlQuery | DESCRIBE} filtered to the required columns and checks that the time column is a date and the summary
     * count column, and that a bucketed time column fits {@code bucketSpan} and {@code frequency}. Tolerates deferred source existence.
     */
    public void validateQuery(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        @Nullable String summaryCountField,
        TimeValue bucketSpan,
        @Nullable TimeValue frequency,
        ActionListener<Boolean> listener,
        @Nullable String datafeedId
    ) {
        describe(
            client,
            headers,
            esqlQuery,
            projectRouting,
            timeField,
            summaryCountField,
            bucketSpan,
            frequency,
            Phase.PUT,
            listener.map(remoteSources -> Boolean.TRUE),
            datafeedId
        );
    }

    /**
     * The probe for datafeed start. Runs the same {@code DESCRIBE} as {@link #validateQuery} but never defers: a source that does not
     * exist, an unreachable remote and a skipped remote without columns fail the start, as a DSL datafeed fails when it cannot read
     * its indices. On success returns the remote sources as {@code cluster:index_expression}, so start can run the remote-cluster
     * checks of a DSL datafeed. The local cluster and the cross-project {@code _origin} qualifier are not remote.
     */
    public void validateAtStart(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        @Nullable String summaryCountField,
        TimeValue bucketSpan,
        @Nullable TimeValue frequency,
        ActionListener<List<String>> listener,
        @Nullable String datafeedId
    ) {
        describe(
            client,
            headers,
            esqlQuery,
            projectRouting,
            timeField,
            summaryCountField,
            bucketSpan,
            frequency,
            Phase.START,
            listener,
            datafeedId
        );
    }

    /** PUT defers a source that does not exist, an unreachable remote and skipped clusters, as for a DSL datafeed. Start fails. */
    private enum Phase {
        PUT,
        START
    }

    /** Runs the {@code DESCRIBE} probe and returns the remote sources, or an empty list when PUT defers. */
    private void describe(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        @Nullable String summaryCountField,
        TimeValue bucketSpan,
        @Nullable TimeValue frequency,
        Phase phase,
        ActionListener<List<String>> listener,
        @Nullable String datafeedId
    ) {
        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            List<String> remote = new ArrayList<>();
            try {
                if (phase == Phase.PUT) {
                    List<String> unavailable = unavailableClustersWithoutDescription(response);
                    if (unavailable.isEmpty() == false) {
                        logger.debug(
                            () -> "[" + datafeedId + "] ES|QL DESCRIBE probe skipped clusters " + unavailable + ". Deferring to start."
                        );
                        listener.onResponse(List.of());
                        return;
                    }
                }
                checkResponse(response, timeField, summaryCountField, bucketSpan, frequency, datafeedId);
                for (EsqlClusterInfo cluster : response.response().clusters()) {
                    String alias = cluster.clusterAlias();
                    if (alias.equals(EsqlClusterInfo.LOCAL_CLUSTER_ALIAS) || alias.equals(ProjectRoutingResolver.ORIGIN)) {
                        continue;
                    }
                    // An unknown expression still names a remote cluster, so keep it for the cluster-level checks.
                    remote.add(
                        alias + RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR + Objects.requireNonNullElse(
                            cluster.indexExpression(),
                            "*"
                        )
                    );
                }
            } catch (Exception e) {
                // Fail directly: the failure handler below would defer a column check failure that looks like a missing source.
                listener.onFailure(e);
                return;
            }
            listener.onResponse(List.copyOf(remote));
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            // A source that does not exist yet or an unreachable remote does not fail PUT, same as a DSL datafeed. Start checks remotes.
            if (phase == Phase.PUT
                && (isDeferredExistenceFailure(cause) || org.elasticsearch.ExceptionsHelper.isRemoteUnavailableException(e))) {
                logger.debug(() -> "[" + datafeedId + "] ES|QL DESCRIBE probe failed with [" + cause + "]. Deferring to runtime.");
                listener.onResponse(List.of());
            } else {
                listener.onFailure(e);
            }
        });
        describeRequiredColumns(queryRunner, client, headers, esqlQuery, projectRouting, timeField, summaryCountField, responseListener);
    }

    /**
     * Runs {@code esqlQuery | DESCRIBE} filtered to the time column and, when given, the summary count column. The datafeed start probe
     * and the task node's time column interval read share it.
     */
    static void describeRequiredColumns(
        EsqlQueryRunner queryRunner,
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        @Nullable String summaryCountField,
        ActionListener<EsqlQueryResponse> listener
    ) {
        queryRunner.executeAsync(
            client,
            EsqlDataExtractorQueryUtils.appendPipe(esqlQuery, DESCRIBE_REQUIRED_COLUMNS),
            null,
            headers,
            projectRouting,
            describeParams(timeField, summaryCountField),
            ExecutionMetadata.CCS_ONLY,
            listener
        );
    }

    /** Without a summary count column, the time column fills both names of {@link #DESCRIBE_REQUIRED_COLUMNS}. */
    private static List<EsqlQueryParam> describeParams(String timeField, @Nullable String summaryCountField) {
        return List.of(
            new EsqlQueryParam("describeTimeColumn", timeField, VALUE),
            new EsqlQueryParam("describeCountColumn", Objects.requireNonNullElse(summaryCountField, timeField), VALUE)
        );
    }

    /**
     * With {@code skip_unavailable=true}, ES|QL skips an unreachable remote or one that matches no index instead of failing. When
     * no source can be described, the response has no DESCRIBE rows. It has either only a placeholder column, or the DESCRIBE columns
     * with no rows. The local cluster can report {@code SUCCESSFUL} next to a skipped remote, so a placeholder column alone is enough.
     * With the DESCRIBE columns, a cluster that ran the query means a source was described, so empty rows are a missing required
     * column and not an unavailable source. Returns the skipped or failed clusters when no source could be described, and an empty
     * list otherwise.
     */
    private static List<String> unavailableClustersWithoutDescription(EsqlQueryResponse response) {
        if (response.response().rows().iterator().hasNext()) {
            return List.of();
        }
        List<EsqlClusterInfo> clusters = response.response().clusters();
        boolean anyClusterRanQuery = clusters.stream()
            .anyMatch(c -> c.status() == EsqlClusterInfo.Status.SUCCESSFUL || c.status() == EsqlClusterInfo.Status.PARTIAL);
        if (hasDescribeColumns(response.response().columns()) && anyClusterRanQuery) {
            return List.of();
        }
        return clusters.stream()
            .filter(c -> c.status() == EsqlClusterInfo.Status.SKIPPED || c.status() == EsqlClusterInfo.Status.FAILED)
            .map(EsqlClusterInfo::clusterAlias)
            .toList();
    }

    private static boolean hasDescribeColumns(List<? extends ColumnInfo> header) {
        return header.stream().anyMatch(c -> "name".equals(c.name())) && header.stream().anyMatch(c -> "type".equals(c.name()));
    }

    /**
     * The checks a {@code DESCRIBE} response must pass whenever a datafeed runs: no unavailable remote cluster, a date time column,
     * the summary count column, and a time bucket that divides {@code bucketSpan} and {@code frequency}. Shared by the datafeed start
     * on the master and the task node. Returns the time column's bucket length in milliseconds, or 0 when the column is not bucketed.
     */
    static long checkResponse(
        EsqlQueryResponse response,
        String timeField,
        @Nullable String summaryCountField,
        TimeValue bucketSpan,
        @Nullable TimeValue frequency,
        @Nullable String datafeedId
    ) {
        List<String> unavailable = unavailableClustersWithoutDescription(response);
        if (unavailable.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(
                    Messages.DATAFEED_ESQL_CLUSTERS_UNAVAILABLE,
                    esqlDatafeedSubject(datafeedId),
                    unavailable,
                    summaryCountField == null ? List.of(timeField) : List.of(timeField, summaryCountField)
                )
            );
        }
        long timeBucketMillis = checkDescription(EsqlQueryDescription.from(response.response()), timeField, summaryCountField, datafeedId);
        checkTimeBucketFitsJob(timeBucketMillis, timeField, bucketSpan, frequency, datafeedId);
        return timeBucketMillis;
    }

    /**
     * The datafeed windows are aligned to the time bucket and each row is posted at its bucket start, so a bucket must not cross a
     * job bucket and a realtime cycle must end on a bucket boundary. The same rules apply to the histogram interval of a datafeed
     * with aggregations.
     */
    static void checkTimeBucketFitsJob(
        long timeBucketMillis,
        String timeField,
        TimeValue bucketSpan,
        @Nullable TimeValue frequency,
        @Nullable String datafeedId
    ) {
        if (timeBucketMillis <= 0) {
            return;
        }
        String subject = esqlDatafeedSubject(datafeedId);
        // The interval has no configured unit, so toString() gives the readable form (15m), where getStringRep() gives 900000ms.
        String interval = TimeValue.timeValueMillis(timeBucketMillis).toString();
        if (bucketSpan.millis() % timeBucketMillis != 0) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(
                    Messages.DATAFEED_ESQL_TIME_BUCKET_NOT_DIVISOR_OF_BUCKET_SPAN,
                    subject,
                    timeField,
                    interval,
                    bucketSpan.getStringRep()
                )
            );
        }
        if (frequency != null && frequency.millis() % timeBucketMillis != 0) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(
                    Messages.DATAFEED_ESQL_FREQUENCY_NOT_MULTIPLE_OF_TIME_BUCKET,
                    subject,
                    frequency.getStringRep(),
                    interval,
                    timeField
                )
            );
        }
    }

    static long checkDescription(
        EsqlQueryDescription description,
        String timeField,
        @Nullable String summaryCountField,
        @Nullable String datafeedId
    ) {
        Map<String, String> columnTypes = description.columnTypes();
        checkRequiredColumns(columnTypes, timeField, summaryCountField, datafeedId);
        String subject = esqlDatafeedSubject(datafeedId);
        String timeType = columnTypes.get(timeField);
        assert timeType != null : "checkRequiredColumns accepted a missing time column [" + timeField + "]";
        if (EsqlDatafeedColumnValues.isDateColumnType(timeType) == false) {
            throw new IllegalArgumentException(
                Messages.getMessage(Messages.DATAFEED_ESQL_TIME_COLUMN_NOT_DATE, subject, timeField, timeType)
            );
        }
        return timeBucketMillis(description, timeField, datafeedId);
    }

    /**
     * The length of the interval {@code description} reports for {@code timeField}, or 0 when the column is not bucketed. Fails for a
     * calendar interval, which has no fixed length, for an interval this parser does not know, and for a bucket that is not in UTC.
     */
    static long timeBucketMillis(EsqlQueryDescription description, String timeField, @Nullable String datafeedId) {
        String interval = description.columnIntervals().get(timeField);
        if (interval == null) {
            return 0;
        }
        String timeZone = description.columnTimeZones().get(timeField);
        if (timeZone != null && UTC.equals(timeZone) == false) {
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(Messages.DATAFEED_ESQL_TIME_BUCKET_NOT_UTC, esqlDatafeedSubject(datafeedId), timeField, timeZone)
            );
        }
        return fixedIntervalMillis(interval).orElseThrow(
            () -> ExceptionsHelper.badRequestException(
                Messages.getMessage(
                    isCalendarUnit(interval)
                        ? Messages.DATAFEED_ESQL_TIME_BUCKET_CALENDAR_INTERVAL
                        : Messages.DATAFEED_ESQL_TIME_BUCKET_UNSUPPORTED_INTERVAL,
                    esqlDatafeedSubject(datafeedId),
                    timeField,
                    interval
                )
            )
        );
    }

    private static boolean isCalendarUnit(String interval) {
        ParsedInterval parsed = ParsedInterval.parse(interval);
        return parsed != null
            && (parsed.unit.equals("week") || parsed.unit.equals("month") || parsed.unit.equals("quarter") || parsed.unit.equals("year"));
    }

    /** The singular unit of an interval as {@code DESCRIBE} writes it, and its size, or -1 when the form is not {@code <size> <unit>}. */
    private record ParsedInterval(long size, String unit) {
        static ParsedInterval parse(String interval) {
            String[] parts = interval.trim().split(" +");
            String unit = parts[parts.length - 1];
            unit = unit.endsWith("s") ? unit.substring(0, unit.length() - 1) : unit;
            if (parts.length != 2) {
                return new ParsedInterval(-1, unit);
            }
            try {
                return new ParsedInterval(Long.parseLong(parts[0]), unit);
            } catch (NumberFormatException e) {
                return new ParsedInterval(-1, unit);
            }
        }
    }

    /**
     * Parses an interval as {@code DESCRIBE} writes it, {@code <size> <unit>} with a plural unit when the size is not 1, for example
     * {@code 1 hour} or {@code 90 minutes}. Empty for a calendar interval ({@code month}, {@code quarter}, {@code year}) or a unit
     * this parser does not know, because its length depends on the date. A {@code week} is empty too: ES|QL rounds it to Monday, but
     * the datafeed windows are multiples of the interval from the Unix epoch, which was a Thursday. A {@code day} is fixed because
     * {@link #timeBucketMillis} rejects a bucket that is not in UTC.
     */
    static OptionalLong fixedIntervalMillis(String interval) {
        ParsedInterval parsed = ParsedInterval.parse(interval);
        long unitMillis = switch (parsed.unit) {
            case "millisecond" -> 1;
            case "second" -> TimeUnit.SECONDS.toMillis(1);
            case "minute" -> TimeUnit.MINUTES.toMillis(1);
            case "hour" -> TimeUnit.HOURS.toMillis(1);
            case "day" -> TimeUnit.DAYS.toMillis(1);
            default -> 0;
        };
        return unitMillis == 0 || parsed.size <= 0 ? OptionalLong.empty() : OptionalLong.of(Math.multiplyExact(parsed.size, unitMillis));
    }

    /**
     * An ES|QL datafeed update needs the probe when its project routing, credential or frequency changed, or when it newly requires a
     * summary count column. Other updates cannot change the probe outcome. A PUT always probes and does not use this method.
     */
    public static boolean updateNeedsProbe(DatafeedConfig current, DatafeedConfig updated, Job job) {
        if (updated.getEsqlQuery() == null) {
            return false;
        }
        boolean searchScopeChanged = Objects.equals(current.getProjectRouting(), updated.getProjectRouting()) == false
            || Objects.equals(credentialId(current), credentialId(updated)) == false;
        // The frequency must stay a multiple of the time column's interval.
        boolean frequencyChanged = Objects.equals(current.getFrequency(), updated.getFrequency()) == false;
        if (searchScopeChanged || frequencyChanged) {
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
        String limitZeroQuery = EsqlDataExtractorQueryUtils.appendPipe(esqlQuery, LIMIT_ZERO);

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
        for (Iterable<Object> row : rows) {
            validateEmittedTimeValueInSourceWindow(
                jobId,
                emittedTimeField,
                valueAt(row, emittedTimeColumnIndex),
                sourceWindowStart,
                sourceWindowEnd
            );
        }
    }

    /**
     * Validates and converts an emitted ES|QL timestamp that has already been materialized outside
     * the typed ES|QL response. Values are date/date_nanos string representations.
     */
    public static long validateEmittedTimeValueInSourceWindow(
        String jobId,
        String emittedTimeField,
        Object rawValue,
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
            emittedTimeMillis = EsqlDatafeedColumnValues.toEpochMillis(rawValue);
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
