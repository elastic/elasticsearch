/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.Objects;

/**
 * The response of {@link PrometheusQueryTransportAction}: the response of the ES|QL query it ran, with what the REST handler
 * needs to render it that is only known once the PromQL query is parsed.
 * <p>
 * It holds no resources of its own: its references are those of the ES|QL response, which, as any ES|QL response, is released
 * by the action once the listener it completes returns.
 */
public class PrometheusQueryResponse extends ActionResponse {

    private final EsqlQueryResponse esqlResponse;
    @Nullable
    private final String resultType;
    @Nullable
    private final String stringValue;

    /**
     * @param resultType the Prometheus result type of a query response, {@code null} for the other endpoints
     * @param stringValue the value of a string literal query, which is its response, otherwise {@code null}
     */
    PrometheusQueryResponse(EsqlQueryResponse esqlResponse, @Nullable String resultType, @Nullable String stringValue) {
        this.esqlResponse = Objects.requireNonNull(esqlResponse);
        this.resultType = resultType;
        this.stringValue = stringValue;
    }

    EsqlQueryResponse esqlResponse() {
        return esqlResponse;
    }

    @Nullable
    String resultType() {
        return resultType;
    }

    @Nullable
    String stringValue() {
        return stringValue;
    }

    /**
     * Completes {@code listener} with the ES|QL response, for the endpoints whose response is built from it alone. Should it
     * throw while handling the response, it is failed with that exception, as when it listened to ES|QL directly.
     */
    static ActionListener<PrometheusQueryResponse> esqlResponseListener(ActionListener<EsqlQueryResponse> listener) {
        return ActionListener.wrap(response -> listener.onResponse(response.esqlResponse()), listener::onFailure);
    }

    @Override
    public void incRef() {
        esqlResponse.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return esqlResponse.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return esqlResponse.decRef();
    }

    @Override
    public boolean hasReferences() {
        return esqlResponse.hasReferences();
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
    }
}
