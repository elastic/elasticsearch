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
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
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
    private boolean includeCCSMetadata;
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
     * "Tables" provided in the request for use with things like {@code LOOKUP}.
     */
    private final Map<String, Map<String, Column>> tables = new TreeMap<>();

    static EsqlQueryRequest syncEsqlQueryRequest() {
        return new EsqlQueryRequest(false);
    }

    static EsqlQueryRequest asyncEsqlQueryRequest() {
        return new EsqlQueryRequest(true);
    }

    private EsqlQueryRequest(boolean async) {
        this.async = async;
    }

    public EsqlQueryRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(query) == false) {
            validationException = addValidationError("[" + RequestXContent.QUERY_FIELD + "] is required", validationException);
        }

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

    public EsqlQueryRequest() {}

    public void query(String query) {
        this.query = query;
    }

    @Override
    public String query() {
        return query;
    }

    public boolean async() {
        return async;
    }

    public void columnar(boolean columnar) {
        this.columnar = columnar;
    }

    public boolean columnar() {
        return columnar;
    }

    /**
     * Enable profiling, sacrificing performance to return information about
     * what operations are taking the most time.
     */
    public void profile(boolean profile) {
        this.profile = profile;
    }

    public void includeCCSMetadata(boolean include) {
        this.includeCCSMetadata = include;
    }

    public boolean includeCCSMetadata() {
        return includeCCSMetadata;
    }

    /**
     * Is profiling enabled?
     */
    public boolean profile() {
        return profile;
    }

    public void locale(Locale locale) {
        this.locale = locale;
    }

    public Locale locale() {
        return locale;
    }

    public void filter(QueryBuilder filter) {
        this.filter = filter;
    }

    @Override
    public QueryBuilder filter() {
        return filter;
    }

    public void pragmas(QueryPragmas pragmas) {
        this.pragmas = pragmas;
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

    public void waitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
    }

    public TimeValue keepAlive() {
        return keepAlive;
    }

    public void keepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean keepOnCompletion() {
        return keepOnCompletion;
    }

    public void keepOnCompletion(boolean keepOnCompletion) {
        this.keepOnCompletion = keepOnCompletion;
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
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // Pass the query as the description
        return new CancellableTask(id, type, action, query, parentTaskId, headers);
    }

    // Setter for tests
    void onSnapshotBuild(boolean onSnapshotBuild) {
        this.onSnapshotBuild = onSnapshotBuild;
    }

    void acceptedPragmaRisks(boolean accepted) {
        this.acceptedPragmaRisks = accepted;
    }
}
