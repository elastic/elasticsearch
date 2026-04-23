/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.source.FromGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a parenthesized subquery suitable for embedding in another query, e.g. {@code (FROM idx)}
 * inside a {@code FROM (...)} source list. Designed to be reusable by future call-sites such as
 * {@code WHERE col IN (subquery)}.
 * <p>
 *     Generation is performed using an inner {@link EsqlQueryGenerator.Executor} that runs each
 *     generated command incrementally and rejects any addition that throws or yields no schema, so
 *     the returned query is guaranteed to execute successfully on its own.
 * </p>
 */
public final class SubqueryGenerator {

    private static final int INNER_PIPE_DEPTH = 5;

    private SubqueryGenerator() {}

    public record SubqueryResult(String queryText, List<Column> outputSchema) {}

    /**
     * Returns a parenthesized subquery and its output schema, or {@code null} if generation or
     * validation failed (in which case the caller should fall back to a non-subquery alternative).
     */
    public static SubqueryResult build(GenerationContext outerContext, CommandGenerator.QuerySchema schema, QueryExecutor queryExecutor) {
        GenerationContext innerContext = outerContext.withSubqueryDepth(outerContext.subqueryDepth() + 1);
        InnerExecutor inner = new InnerExecutor(queryExecutor);

        EsqlQueryGenerator.generatePipeline(INNER_PIPE_DEPTH, FromGenerator.INSTANCE, schema, inner, false, queryExecutor, innerContext);

        QueryExecuted last = inner.lastResult;
        if (last == null || last.exception() != null || last.outputSchema() == null || last.outputSchema().isEmpty()) {
            return null;
        }
        return new SubqueryResult("(" + last.query() + ")", last.outputSchema());
    }

    /**
     * Inner Executor that runs each generated command incrementally and aborts on the first failure,
     * mirroring the discipline used by {@code ForkGenerator} for branch validation.
     */
    private static final class InnerExecutor implements EsqlQueryGenerator.Executor {

        private final QueryExecutor queryExecutor;
        private final List<CommandGenerator.CommandDescription> previousCommands = new ArrayList<>();
        private List<Column> currentSchema = List.of();
        private QueryExecuted lastResult;
        private boolean continueExecuting = true;

        InnerExecutor(QueryExecutor queryExecutor) {
            this.queryExecutor = queryExecutor;
        }

        @Override
        public void run(CommandGenerator generator, CommandGenerator.CommandDescription current) {
            String fullQuery = lastResult == null ? current.commandString() : lastResult.query() + current.commandString();
            int depth = lastResult == null ? 0 : lastResult.depth();
            QueryExecuted result = queryExecutor.execute(fullQuery, depth);
            lastResult = result;
            if (result.exception() != null || result.outputSchema() == null || result.outputSchema().isEmpty()) {
                continueExecuting = false;
                currentSchema = List.of();
                return;
            }
            previousCommands.add(current);
            currentSchema = result.outputSchema();
        }

        @Override
        public List<CommandGenerator.CommandDescription> previousCommands() {
            return previousCommands;
        }

        @Override
        public boolean continueExecuting() {
            return continueExecuting;
        }

        @Override
        public List<Column> currentSchema() {
            return currentSchema;
        }
    }
}
