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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.esql.parser.TypedParamValue;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.version.EsqlVersion;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class EsqlQueryRequest extends org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest implements CompositeIndicesRequest {

    public static TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);
    public static TimeValue DEFAULT_WAIT_FOR_COMPLETION = TimeValue.timeValueSeconds(1);

    private boolean async;

    private String esqlVersion;
    private String query;
    private boolean columnar;
    private boolean profile;
    private Locale locale;
    private QueryBuilder filter;
    private QueryPragmas pragmas = new QueryPragmas(Settings.EMPTY);
    private List<TypedParamValue> params = List.of();
    private TimeValue waitForCompletionTimeout = DEFAULT_WAIT_FOR_COMPLETION;
    private TimeValue keepAlive = DEFAULT_KEEP_ALIVE;
    private boolean keepOnCompletion;
    private boolean onSnapshotBuild = Build.current().isSnapshot();

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
        if (Strings.hasText(esqlVersion) == false) {
            // TODO: make this required
            // "https://github.com/elastic/elasticsearch/issues/104890"
            // validationException = addValidationError(invalidVersion("is required"), validationException);
        } else {
            EsqlVersion version = EsqlVersion.parse(esqlVersion);
            if (version == null) {
                validationException = addValidationError(invalidVersion("has invalid value [" + esqlVersion + "]"), validationException);
            } else if (version == EsqlVersion.SNAPSHOT && onSnapshotBuild == false) {
                validationException = addValidationError(
                    invalidVersion("with value [" + esqlVersion + "] only allowed in snapshot builds"),
                    validationException
                );
            }
        }
        if (Strings.hasText(query) == false) {
            validationException = addValidationError("[" + RequestXContent.QUERY_FIELD + "] is required", validationException);
        }
        if (onSnapshotBuild == false && pragmas.isEmpty() == false) {
            validationException = addValidationError(
                "[" + RequestXContent.PRAGMA_FIELD + "] only allowed in snapshot builds",
                validationException
            );
        }
        return validationException;
    }

    private static String invalidVersion(String reason) {
        return "["
            + RequestXContent.ESQL_VERSION_FIELD
            + "] "
            + reason
            + ", latest available version is ["
            + EsqlVersion.latestReleased().versionStringWithoutEmoji()
            + "]";
    }

    public EsqlQueryRequest() {}

    public void esqlVersion(String esqlVersion) {
        this.esqlVersion = esqlVersion;
    }

    @Override
    public String esqlVersion() {
        return esqlVersion;
    }

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

    public List<TypedParamValue> params() {
        return params;
    }

    public void params(List<TypedParamValue> params) {
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

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // Pass the query as the description
        return new CancellableTask(id, type, action, query, parentTaskId, headers);
    }

    // Setter for tests
    void onSnapshotBuild(boolean onSnapshotBuild) {
        this.onSnapshotBuild = onSnapshotBuild;
    }
}
