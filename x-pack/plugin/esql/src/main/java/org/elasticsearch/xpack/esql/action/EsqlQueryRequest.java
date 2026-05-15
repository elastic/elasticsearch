/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.QuerySettings.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plugin.EsqlQueryStatus;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EsqlQueryRequest extends org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest implements CompositeIndicesRequest {

    public static TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);
    public static TimeValue DEFAULT_WAIT_FOR_COMPLETION = TimeValue.timeValueSeconds(1);

    private boolean async;

    private String query;
    private boolean columnar;
    private boolean profile;
    private Boolean includeCCSMetadata;
    private Boolean includeExecutionMetadata;
    private Locale locale;
    private QueryBuilder filter;
    private QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
    private QueryParams params = new QueryParams();
    private TimeValue waitForCompletionTimeout = DEFAULT_WAIT_FOR_COMPLETION;
    private TimeValue keepAlive = DEFAULT_KEEP_ALIVE;
    private boolean keepOnCompletion;
    private boolean onSnapshotBuild = Build.current().isSnapshot();
    private boolean acceptedPragmaRisks = false;
    private Boolean allowPartialResults = null;

    /**
     * Body-supplied values for SETtings, keyed by registry definition. Populated by {@link RequestXContent} from the
     * {@code settings.{}} block and from any registry-declared additional bindings. Read by {@code EsqlSession} during
     * resolution, where it sits at lower precedence than query SETs.
     */
    private final Map<QuerySettingDef<?>, Object> requestSettings = new HashMap<>();

    /**
     * Values supplied via the canonical {@code settings.{}} block, accumulated during JSON parsing.
     * Merged into {@link #requestSettings} (overwriting any additional-binding values) by
     * {@link #applyCanonicalRequestSettings()} after parsing completes. This implements the
     * "canonical wins over additional binding" precedence rule without depending on JSON field order.
     */
    private final Map<QuerySettingDef<?>, Object> canonicalRequestSettings = new HashMap<>();

    /**
     * "Tables" provided in the request for use with things like {@code LOOKUP}.
     */
    private final Map<String, Map<String, Column>> tables = new TreeMap<>();

    public static EsqlQueryRequest syncEsqlQueryRequest(String query) {
        return new EsqlQueryRequest(false, query);
    }

    public static EsqlQueryRequest asyncEsqlQueryRequest(String query) {
        return new EsqlQueryRequest(true, query);
    }

    EsqlQueryRequest(boolean async, String query) {
        this.async = async;
        this.query = query;
    }

    /**
     * Copy constructor. Copies all fields from {@code source}. Subclasses that need to override
     * specific fields (e.g. {@link PreparedEsqlQueryRequest} overrides {@code query}) should do
     * so after calling this constructor. If a new field is added to this class, it must also be
     * added here.
     */
    EsqlQueryRequest(EsqlQueryRequest source) {
        this.async = source.async;
        this.query = source.query;
        this.columnar = source.columnar;
        this.profile = source.profile;
        this.includeCCSMetadata = source.includeCCSMetadata;
        this.includeExecutionMetadata = source.includeExecutionMetadata;
        this.locale = source.locale;
        this.filter = source.filter;
        this.pragmas = source.pragmas;
        this.params = source.params;
        this.waitForCompletionTimeout = source.waitForCompletionTimeout;
        this.keepAlive = source.keepAlive;
        this.keepOnCompletion = source.keepOnCompletion;
        this.onSnapshotBuild = source.onSnapshotBuild;
        this.acceptedPragmaRisks = source.acceptedPragmaRisks;
        this.allowPartialResults = source.allowPartialResults;
        this.requestSettings.putAll(source.requestSettings);
        this.canonicalRequestSettings.putAll(source.canonicalRequestSettings);
        this.tables.putAll(source.tables);
    }

    public EsqlQueryRequest() {}

    public EsqlQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = validateQuery();
        if (onSnapshotBuild == false) {
            if (pragmas.isEmpty() == false && acceptedPragmaRisks == false) {
                validationException = addValidationError(
                    "[" + RequestXContent.PRAGMA_FIELD + "] only allowed in snapshot builds",
                    validationException
                );
            }
            if (tables.isEmpty() == false) {
                validationException = addValidationError(
                    "[" + RequestXContent.TABLES_FIELD + "] only allowed in snapshot builds",
                    validationException
                );
            }
        }
        return validationException;
    }

    protected ActionRequestValidationException validateQuery() {
        if (Strings.hasText(query) == false) {
            return addValidationError("[" + RequestXContent.QUERY_FIELD + "] is required", null);
        }
        return null;
    }

    public EsqlQueryRequest query(String query) {
        this.query = query;
        return this;
    }

    @Override
    @Nullable
    public String query() {
        return query;
    }

    /**
     * Returns a non-null human-readable description of the query for logging, task descriptions, and error messages.
     * For regular requests this is the same as {@link #query()}. Overridden by {@link PreparedEsqlQueryRequest}
     * to return a display string when there is no query text.
     */
    public String queryDescription() {
        return query();
    }

    /**
     * Parses the query string into an {@link EsqlStatement} and validates its settings.
     * Overridden by {@link PreparedEsqlQueryRequest} to return a pre-built statement directly.
     */
    public EsqlStatement parse(EsqlParser parser, SettingsValidationContext settingsValidationCtx, InferenceSettings inferenceSettings) {
        EsqlStatement statement = parser.parse(query(), params(), inferenceSettings);
        QuerySettings.validate(statement, settingsValidationCtx);
        return statement;
    }

    public boolean async() {
        return async;
    }

    public EsqlQueryRequest columnar(boolean columnar) {
        this.columnar = columnar;
        return this;
    }

    public boolean columnar() {
        return columnar;
    }

    /**
     * Enable profiling, sacrificing performance to return information about
     * what operations are taking the most time.
     */
    public EsqlQueryRequest profile(boolean profile) {
        this.profile = profile;
        return this;
    }

    public EsqlQueryRequest includeCCSMetadata(Boolean include) {
        this.includeCCSMetadata = include;
        return this;
    }

    public Boolean includeCCSMetadata() {
        return includeCCSMetadata;
    }

    public EsqlQueryRequest includeExecutionMetadata(Boolean include) {
        this.includeExecutionMetadata = include;
        return this;
    }

    public Boolean includeExecutionMetadata() {
        return includeExecutionMetadata;
    }

    /**
     * Is profiling enabled?
     */
    public boolean profile() {
        return profile;
    }

    public void timeZone(ZoneId timeZone) {
        setRequestSetting(QuerySettings.TIME_ZONE, timeZone);
    }

    public ZoneId timeZone() {
        return getRequestSetting(QuerySettings.TIME_ZONE);
    }

    public void locale(Locale locale) {
        this.locale = locale;
    }

    public Locale locale() {
        return locale;
    }

    public EsqlQueryRequest filter(QueryBuilder filter) {
        this.filter = filter;
        return this;
    }

    @Override
    public QueryBuilder filter() {
        return filter;
    }

    public EsqlQueryRequest pragmas(QueryPragmas pragmas) {
        this.pragmas = pragmas;
        return this;
    }

    public QueryPragmas pragmas() {
        return pragmas;
    }

    public QueryParams params() {
        return params;
    }

    public void params(QueryParams params) {
        this.params = params;
    }

    public TimeValue waitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public EsqlQueryRequest waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
        return this;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    public EsqlQueryRequest keepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public boolean keepOnCompletion() {
        return keepOnCompletion;
    }

    public EsqlQueryRequest keepOnCompletion(boolean keepOnCompletion) {
        this.keepOnCompletion = keepOnCompletion;
        return this;
    }

    /**
     * Add a "table" to the request for use with things like {@code LOOKUP}.
     */
    public void addTable(String name, Map<String, Column> columns) {
        for (Column c : columns.values()) {
            if (false == c.values().blockFactory().breaker() instanceof NoopCircuitBreaker) {
                throw new AssertionError("block tracking not supported on tables parameter");
            }
        }
        Iterator<Column> itr = columns.values().iterator();
        if (itr.hasNext()) {
            int firstSize = itr.next().values().getPositionCount();
            while (itr.hasNext()) {
                int size = itr.next().values().getPositionCount();
                if (size != firstSize) {
                    throw new IllegalArgumentException("mismatched column lengths: was [" + size + "] but expected [" + firstSize + "]");
                }
            }
        }
        var prev = tables.put(name, columns);
        if (prev != null) {
            Releasables.close(prev.values());
            throw new IllegalArgumentException("duplicate table for [" + name + "]");
        }
    }

    public Map<String, Map<String, Column>> tables() {
        return tables;
    }

    public Boolean allowPartialResults() {
        return allowPartialResults;
    }

    public EsqlQueryRequest allowPartialResults(boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
        return this;
    }

    @Override
    public Task createTask(TaskId taskId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        var status = new EsqlQueryStatus(new AsyncExecutionId(UUIDs.randomBase64UUID(), taskId), keepAlive);
        return new EsqlQueryRequestTask(queryDescription(), taskId.getId(), type, action, parentTaskId, headers, status);
    }

    private static class EsqlQueryRequestTask extends CancellableTask {
        private final Status status;

        EsqlQueryRequestTask(
            String query,
            long id,
            String type,
            String action,
            TaskId parentTaskId,
            Map<String, String> headers,
            EsqlQueryStatus status
        ) {
            // Pass the query as the description
            super(id, type, action, query, parentTaskId, headers);
            this.status = status;
        }

        @Override
        public Status getStatus() {
            return status;
        }
    }

    // Setter for tests
    void onSnapshotBuild(boolean onSnapshotBuild) {
        this.onSnapshotBuild = onSnapshotBuild;
    }

    void acceptedPragmaRisks(boolean accepted) {
        this.acceptedPragmaRisks = accepted;
    }

    public EsqlQueryRequest projectRouting(String projectRouting) {
        setRequestSetting(QuerySettings.PROJECT_ROUTING, projectRouting);
        return this;
    }

    public String projectRouting() {
        return getRequestSetting(QuerySettings.PROJECT_ROUTING);
    }

    public EsqlQueryRequest approximation(ApproximationSettings approximation) {
        setRequestSetting(QuerySettings.APPROXIMATION, approximation);
        return this;
    }

    public ApproximationSettings approximation() {
        return getRequestSetting(QuerySettings.APPROXIMATION);
    }

    /**
     * The body-supplied SET values keyed by registry definition. Lower precedence than query-string SET.
     * Reflects the final merged view after {@link #applyCanonicalRequestSettings()} runs at end of parsing.
     */
    public Map<QuerySettingDef<?>, Object> requestSettings() {
        return requestSettings;
    }

    /**
     * Map populated by {@link RequestXContent} while parsing the {@code settings.{}} block. Not for external
     * consumption — call {@link #requestSettings()} after parsing instead.
     */
    public Map<QuerySettingDef<?>, Object> canonicalRequestSettings() {
        return canonicalRequestSettings;
    }

    /**
     * Merge canonical {@code settings.{}} values into {@link #requestSettings}, overwriting any values supplied via
     * additional bindings (legacy top-level fields). Called once by {@link RequestXContent} after parsing.
     */
    public void applyCanonicalRequestSettings() {
        if (canonicalRequestSettings.isEmpty()) {
            return;
        }
        requestSettings.putAll(canonicalRequestSettings);
        canonicalRequestSettings.clear();
    }

    @SuppressWarnings("unchecked")
    private <T> T getRequestSetting(QuerySettingDef<T> def) {
        return (T) requestSettings.get(def);
    }

    private <T> void setRequestSetting(QuerySettingDef<T> def, T value) {
        if (value == null) {
            requestSettings.remove(def);
        } else {
            requestSettings.put(def, value);
        }
    }
}
