/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;

import java.util.List;

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

    /**
     * Returns {@code true} if the given failure is a known/allowed error that the test suite tolerates,
     * {@code false} if the failure is unexpected and should propagate.
     * The default implementation treats every failure as unexpected.
     */
    default boolean isAllowedFailure(
        QueryExecuted result,
        List<CommandGenerator.CommandDescription> previousCommands,
        List<Column> currentSchema
    ) {
        return false;
    }

    /**
     * Recomputes the {@link Column#indexMapped()} flags for the output schema of a freshly executed command.
     * <p>
     * A schema returned by {@link #execute} only carries column names and types; every column defaults to
     * {@code indexMapped=true}. This hook lets the executor mark columns that the command <em>derived</em>
     * (e.g. {@code EVAL}, {@code GROK}, {@code DISSECT}, {@code RENAME}, {@code MV_EXPAND}, {@code STATS},
     * {@code REGISTERED_DOMAIN}) as {@code indexMapped=false}, inheriting flags from {@code previousSchema} for
     * surviving columns. Correct flags are what stop full-text functions from being generated against derived
     * (non-index-mapped) fields, and what let a failure on such a field be recognized as a known bug.
     * <p>
     * Must be applied after every successful command so the flags thread forward. The default returns
     * {@code newSchema} unchanged (i.e. no flag tracking), matching executors that don't distinguish
     * index-mapped fields.
     */
    default List<Column> updateIndexMapped(
        List<Column> newSchema,
        List<Column> previousSchema,
        CommandGenerator.CommandDescription command
    ) {
        return newSchema;
    }
}
