/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;
import java.util.function.Supplier;

/**
 * A table function whose rows the coordinator produces.
 * <p>
 * The columns are known before the rows are, so the leaf this builds is already resolved and nothing downstream
 * looks anything up. It is the same road {@code ROW a=1} travels: {@code ReplaceRowAsLocalRelation} turns a
 * {@code Row} into a {@code LocalRelation} carrying {@code LocalSupplier.of(new Page(blocks))}, built by
 * {@code BlockUtils}. A subclass never sees a {@link Block}.
 * <p>
 * <b>Asynchronous, because producing coordinator rows is not the same as producing them cheaply.</b> A function
 * answering out of cluster state already in memory and one listing an object store over the network both produce
 * coordinator rows. A function that needs no I/O uses {@link #computed} and never handles a listener.
 * <p>
 * {@link #toRelation} is {@code final}. There is no seam here for a wire format, a node stage or credentials,
 * because a function of this shape has none — not by convention but because the only method it can override
 * cannot express one.
 *
 * <h2>What this deliberately does not do</h2>
 *
 * It does not bind {@code METADATA}. The analyzer's own outer-metadata injection wraps a plan in
 * {@code Eval(null AS field)} for every metadata name absent from a leaf's output, in both the lone and the union
 * forms, so a rows leaf already answers {@code FROM logs-* METADATA _index, SOMETHING()} with NULLs for the names
 * it has nothing to say about. Binding them here would be a second mechanism for the same thing.
 */
public abstract class CoordinatorRowsFunction implements TableFunction {

    /**
     * The columns this invocation produces. Called before {@link #rows}, and must agree with it in order and
     * arity: these columns name the blocks those rows become.
     */
    protected abstract List<Attribute> columns(TableFunctionInvocation call, TableFunctionContext context);

    /** The rows, each in {@link #columns} order. */
    protected abstract void rows(TableFunctionInvocation call, TableFunctionContext context, ActionListener<List<List<Object>>> listener);

    @Override
    public final void toRelation(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        TableFunctionContext context,
        ActionListener<LeafPlan> listener
    ) {
        List<Attribute> columns = columns(call, context);
        rows(call, context, listener.map(rows -> new LocalRelation(call.source(), columns, LocalSupplier.of(page(columns, rows)))));
    }

    /** For a function that answers from memory: no listener, no I/O. */
    protected static void computed(ActionListener<List<List<Object>>> listener, Supplier<List<List<Object>>> rows) {
        ActionListener.completeWith(listener, rows::get);
    }

    /** One column. Here so that every function of this shape builds its attributes the same way. */
    protected static Attribute column(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    /**
     * The declared columns are the only source of each block's type.
     * <p>
     * {@code BlockUtils.fromList} would infer an element type per column from the first non-null value it found,
     * which is a second derivation of what {@link #columns} already states: declare {@code LONG}, hand back an
     * {@code Integer}, and the page carries an {@code INT} block under a {@code LONG} attribute with nothing
     * between the two to notice. A zero-row answer still owes one empty block per column for the same reason,
     * which is why there is no special case for it here.
     */
    private static Page page(List<Attribute> columns, List<List<Object>> rows) {
        BlockUtils.BuilderWrapper[] wrappers = new BlockUtils.BuilderWrapper[columns.size()];
        try {
            for (int i = 0; i < wrappers.length; i++) {
                ElementType type = PlannerUtils.toElementType(columns.get(i).dataType());
                wrappers[i] = BlockUtils.wrapperFor(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, type, rows.size());
            }
            for (List<Object> row : rows) {
                // Checked rather than asserted: a function that disagrees with its own column list would otherwise
                // build a page of a different width than the output it is attached to, and do it in production only.
                if (row.size() != wrappers.length) {
                    throw new IllegalStateException(
                        "table function produced a row of [" + row.size() + "] values for [" + wrappers.length + "] columns"
                    );
                }
                for (int i = 0; i < wrappers.length; i++) {
                    Object value = row.get(i);
                    try {
                        wrappers[i].accept(value);
                    } catch (ClassCastException e) {
                        // The builder is typed from the declaration, so it rejects a value of another Java type. That
                        // is the behaviour we want; the bare cast failure is not, as it names neither the column nor
                        // the declaration it disagreed with.
                        Attribute column = columns.get(i);
                        throw new IllegalStateException(
                            "table function produced ["
                                + (value == null ? "null" : value.getClass().getSimpleName())
                                + "] for column ["
                                + column.name()
                                + "] declared ["
                                + column.dataType().typeName()
                                + "]",
                            e
                        );
                    }
                }
            }
            Block[] blocks = new Block[wrappers.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = wrappers[i].builder().build();
            }
            return new Page(rows.size(), blocks);
        } finally {
            Releasables.close(wrappers);
        }
    }
}
