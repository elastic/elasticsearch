/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;

import java.util.List;

/** Runs a single ES|QL query synchronously for datafeed extraction. */
@FunctionalInterface
interface EsqlSyncQueryRunner {

    EsqlQueryResponse run(String query, QueryBuilder timeFilter, List<EsqlQueryParam> params);
}
