/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.esql.action;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.esql.action.internal.SharedSecrets;

import java.util.List;

public abstract class EsqlQueryRequestBuilder<Request extends EsqlQueryRequest, Response extends EsqlQueryResponse> extends
    ActionRequestBuilder<Request, Response> {

    private final ActionType<Response> action;

    /** Creates a new ES|QL query request builder. */
    public static EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> newRequestBuilder(
        ElasticsearchClient client
    ) {
        return SharedSecrets.getEsqlQueryRequestBuilderAccess().newEsqlQueryRequestBuilder(client);
    }

    // not for direct use
    protected EsqlQueryRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
        super(client, action, request);
        this.action = action;
    }

    public final ActionType<Response> action() {
        return action;
    }

    public abstract EsqlQueryRequestBuilder<Request, Response> query(String query);

    public abstract EsqlQueryRequestBuilder<Request, Response> filter(QueryBuilder filter);

    public abstract EsqlQueryRequestBuilder<Request, Response> allowPartialResults(boolean allowPartialResults);

    public abstract EsqlQueryRequestBuilder<Request, Response> profile(boolean profile);

    public abstract EsqlQueryRequestBuilder<Request, Response> projectRouting(String projectRouting);

    /**
     * When the response reports per-cluster execution metadata through {@link EsqlResponse#clusters()}. Replaces any mode set
     * earlier on this builder.
     */
    public abstract EsqlQueryRequestBuilder<Request, Response> executionMetadata(ExecutionMetadata mode);

    /**
     * Sets an ES|QL query setting by name, with the names and values the REST API accepts under {@code settings} in the request
     * body, for example {@code setting("time_zone", "Europe/Paris")}. The value is what that JSON body would carry: a
     * {@link String}, {@link Number}, {@link Boolean}, or a {@link java.util.Map} or {@link List} of these. {@code null} removes
     * the setting.
     *
     * @throws IllegalArgumentException if no such setting exists, the setting is not accepted in the request body, or the value
     *         does not parse
     */
    public abstract EsqlQueryRequestBuilder<Request, Response> setting(String name, @Nullable Object value);

    public abstract EsqlQueryRequestBuilder<Request, Response> params(List<EsqlQueryParam> params);

    /** When the response reports per-cluster execution metadata ({@link EsqlResponse#clusters()}). */
    public enum ExecutionMetadata {
        /** Never report it. The default. */
        NEVER,
        /** Report it for cross-cluster queries only. Equivalent of the {@code include_ccs_metadata} request parameter. */
        CCS_ONLY,
        /**
         * Report it for every query, including local-only ones. Equivalent of the {@code include_execution_metadata} request
         * parameter.
         */
        ALWAYS
    }

    /**
     * A lightweight representation of a single ES|QL query parameter, used to pass typed values or field-name identifiers
     * into an ES|QL query without splicing them into the query string. {@code classification} determines how the value is
     * substituted by the ES|QL parser:
     * <ul>
     *   <li>{@link ParamClassification#VALUE} — substituted as a typed literal ({@code ?name})</li>
     *   <li>{@link ParamClassification#IDENTIFIER} — substituted as a field/column reference ({@code ??name})
     *   <li>{@link ParamClassification#PATTERN} — substituted as a wildcard field pattern ({@code ??name} containing {@code *})</li>
     * </ul>
     */
    public record EsqlQueryParam(String name, Object value, ParamClassification classification) {
        public enum ParamClassification {
            VALUE,
            IDENTIFIER,
            PATTERN
        }
    }
}
