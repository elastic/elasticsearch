/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * One extraction window of an ES|QL datafeed. {@code timeField} names both the time column of the query output and the source field
 * the request range filter targets; see {@link TimeColumn}.
 */
record EsqlDataExtractorContext(
    String jobId,
    String esqlQuery,
    String timeField,
    long start,
    long end,
    Map<String, String> headers,
    @Nullable String requiredSummaryCountField,
    @Nullable String projectRouting
) {}
