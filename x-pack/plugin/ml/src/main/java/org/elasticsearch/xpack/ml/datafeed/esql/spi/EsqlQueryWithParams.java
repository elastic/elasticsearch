/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;

import java.util.List;
import java.util.Objects;

/** An ES|QL query text together with the parameters it refers to, ready to be sent as one request. */
public record EsqlQueryWithParams(String query, List<EsqlQueryParam> params) {

    public EsqlQueryWithParams {
        Objects.requireNonNull(query);
        params = List.copyOf(params);
    }
}
