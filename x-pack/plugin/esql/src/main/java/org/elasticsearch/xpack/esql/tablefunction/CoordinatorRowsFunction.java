/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;

/**
 * A table function whose answer is known on the coordinator: it writes its columns and rows into {@link TableRows}
 * and this turns them into the relation the rest of the query reads.
 * <p>
 * The rows are materialized while the query is planned and held until it finishes, on the coordinator, with no
 * circuit breaker charged — {@link PlannerUtils#NON_BREAKING_BLOCK_FACTORY}, as {@code ROW} uses. A function whose
 * answer is not small enough for that should read like one, which means handing back an unresolved leaf and letting
 * the ordinary pipeline finish it, rather than extending this.
 */
public abstract class CoordinatorRowsFunction implements TableFunction {

    /**
     * Writes this invocation's columns and rows, then completes {@code listener}. Everything written is released if
     * the listener completes with a failure instead.
     */
    protected abstract void write(
        TableFunctionInvocation call,
        ProjectMetadata projectMetadata,
        TableRows rows,
        ActionListener<Void> listener
    );

    /** For a function that answers from memory, with no listener of its own to complete. */
    protected static void computed(ActionListener<Void> listener, CheckedConsumer<TableRows, Exception> write, TableRows rows) {
        ActionListener.completeWith(listener, () -> {
            write.accept(rows);
            return null;
        });
    }

    @Override
    public final void toRelation(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        ProjectMetadata projectMetadata,
        ActionListener<LeafPlan> listener
    ) {
        TableRows rows = new TableRows(name(), PlannerUtils.NON_BREAKING_BLOCK_FACTORY);
        ActionListener<Void> written = ActionListener.wrap(ignored -> {
            LeafPlan relation;
            try {
                relation = new LocalRelation(call.source(), rows.columns(), LocalSupplier.of(rows.page()));
            } catch (Exception e) {
                rows.close();
                throw e;
            }
            listener.onResponse(relation);
        }, failure -> {
            rows.close();
            listener.onFailure(failure);
        });
        // ActionListener.run so a function that throws rather than completing still releases what it wrote.
        ActionListener.run(written, l -> write(call, projectMetadata, rows, l));
    }
}
