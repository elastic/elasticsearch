/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.PreAnalyzer;
import org.elasticsearch.xpack.esql.session.Configuration;

/**
 * Per-query state threaded into the schema providers so the index-resolution orchestration can be carried out
 * away from the session. Bundles the request-scoped inputs the providers read — the execution info they mutate
 * as clusters resolve, the configuration (project routing, partial-results), the pre-analysis (index patterns,
 * lookup indices, feature flags), the optional request filter, and whether unmapped field indices are tracked.
 */
public record SchemaContext(
    EsqlExecutionInfo executionInfo,
    Configuration configuration,
    PreAnalyzer.PreAnalysis preAnalysis,
    QueryBuilder requestFilter,
    boolean trackUnmappedFieldIndices
) {}
