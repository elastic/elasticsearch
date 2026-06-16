/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.support.IndicesOptions;

import java.util.List;
import java.util.Map;

record EsqlDataExtractorContext(
    String jobId,
    String esqlQuery,
    String timeField,
    long start,
    long end,
    Map<String, String> headers,
    List<String> indices,
    IndicesOptions indicesOptions
) {}
