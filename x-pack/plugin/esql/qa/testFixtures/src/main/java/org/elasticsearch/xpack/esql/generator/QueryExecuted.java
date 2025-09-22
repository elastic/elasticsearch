/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.List;

/**
 * Represents the execution of a query, in the context of generative tests.
 * @param query the query string
 * @param depth how many iterations have been executed in the current generative test sequence
 * @param outputSchema The schema of the output
 * @param result The actual results, as a list of rows.
 * @param exception Null if the query was successful, otherwise the exception that was thrown by the execution.
 */
public record QueryExecuted(String query, int depth, List<Column> outputSchema, List<List<Object>> result, Exception exception) {}
