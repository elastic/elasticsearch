/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;

import java.io.IOException;

/**
 * An {@link EsqlQueryRequest} backed by a pre-built {@link EsqlStatement}, bypassing ES|QL string
 * parsing. Used by internal callers (such as the Prometheus REST endpoints) that construct an
 * {@link EsqlStatement} directly instead of going through ES|QL string construction and parsing.
 *
 * <p>The query string carried by this request is only used for logging and display; it plays no
 * role in execution.
 */
public class PreparedEsqlQueryRequest extends EsqlQueryRequest {

    public static final String PREPARED_QUERY_PREFIX = "[pre-built plan] ";

    private final EsqlStatement statement;
    private final String queryDescription;
    private final boolean analyzeOnly;

    protected PreparedEsqlQueryRequest(boolean async, EsqlStatement statement, String queryDescription, boolean analyzeOnly) {
        super(async, null);
        this.statement = statement;
        this.queryDescription = PREPARED_QUERY_PREFIX + queryDescription;
        this.analyzeOnly = analyzeOnly;
    }

    private PreparedEsqlQueryRequest(EsqlQueryRequest source, EsqlStatement statement, String queryDescription) {
        super(source);
        this.statement = statement;
        this.queryDescription = PREPARED_QUERY_PREFIX + queryDescription;
        this.analyzeOnly = false;
    }

    /**
     * Creates a synchronous request backed by the given pre-built statement.
     */
    public static PreparedEsqlQueryRequest sync(EsqlStatement statement, String queryDescription) {
        return new PreparedEsqlQueryRequest(false, statement, queryDescription, false);
    }

    /**
     * Creates an asynchronous request backed by the given pre-built statement.
     */
    public static PreparedEsqlQueryRequest async(EsqlStatement statement, String queryDescription) {
        return new PreparedEsqlQueryRequest(true, statement, queryDescription, false);
    }

    /**
     * Creates a synchronous request that stops after analysis. The response carries the analyzed columns and no rows.
     * Failures that a regular query would hit during analysis, such as unknown indices or columns, are reported as failures.
     * It is meant for checking a query without running it, so it is not logged and does not count towards query metrics.
     */
    public static PreparedEsqlQueryRequest analyzeOnly(EsqlStatement statement, String queryDescription) {
        return new PreparedEsqlQueryRequest(false, statement, queryDescription, true);
    }

    /**
     * Creates a request backed by {@code statement} with all other properties copied from {@code source}.
     */
    public static PreparedEsqlQueryRequest from(EsqlQueryRequest source, EsqlStatement statement, String queryDescription) {
        return new PreparedEsqlQueryRequest(source, statement, queryDescription);
    }

    /**
     * Returns the pre-built statement carried by this request.
     */
    public EsqlStatement statement() {
        return statement;
    }

    @Override
    @Nullable
    public String query() {
        return null;
    }

    @Override
    public EsqlQueryRequest query(String query) {
        throw new UnsupportedOperationException("PreparedEsqlQueryRequest is backed by a pre-built statement, not a query string");
    }

    @Override
    public String queryDescription() {
        return queryDescription;
    }

    @Override
    public boolean analyzeOnly() {
        return analyzeOnly;
    }

    public String getIndex() {
        return null;
    }

    public String getType() {
        return null;
    }

    @Override
    protected ActionRequestValidationException validateQuery() {
        return null; // no query text for prepared requests — skip the Strings.hasText(query) check
    }

    /**
     * Returns the pre-built statement directly, without invoking the parser, after validating its settings.
     */
    @Override
    public EsqlStatement parse(
        EsqlParser parser,
        SettingsValidationContext settingsValidationCtx,
        InferenceSettings inferenceSettings,
        int maxCommandCount
    ) {
        QuerySettings.validate(statement, settingsValidationCtx);
        return statement;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
