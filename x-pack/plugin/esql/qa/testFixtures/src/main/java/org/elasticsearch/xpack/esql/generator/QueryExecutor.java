/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

/**
 * This is used by generative tests and by command generators,
 * to run queries for test or to run intermediate queries,
 * eg. to compose a complex pipeline to be added to a query being generated (see ForkGenerator).
 */
public interface QueryExecutor {
    /**
     * Execute the given command, returning the results.
     * The depth is used to avoid infinite loops when commands generate sub-queries that are executed.
     * @param query The command to execute
     * @param depth Represents the number of iterations executed in current generative test sequence.
     *              It does not always correspond to the number of commands in the query, because some
     *              command generators may generate more than one command at a time.
     *              This value has to be passed to the resulting QueryExecuted.
     * @return The results of the execution
     */
    QueryExecuted execute(String query, int depth);
}
