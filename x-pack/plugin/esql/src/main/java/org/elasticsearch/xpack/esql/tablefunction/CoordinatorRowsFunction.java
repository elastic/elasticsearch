/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.versionfield.Version;

import java.net.InetAddress;
import java.util.ArrayList;
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
     * The value a block builder should receive for a column, which is not always the value the function handed in.
     * <p>
     * Checking the Java type is not enough for the types whose bytes are an <i>encoding</i> rather than the text.
     * {@code BlockUtils.appendValue}'s {@code BYTES_REF} arm appends whatever bytes it is given, so a
     * {@code BytesRef} holding {@code "1.2.3.4"} under an {@code ip} column would carry seven UTF-8 bytes where
     * the engine reads the sixteen {@link InetAddressPoint} encodes — right element type, wrong bytes, and
     * nothing downstream able to tell.
     * <p>
     * So the seam owns the encoding instead of trusting each function with it: a function hands back the natural
     * Java value and this turns it into the bytes the type is read as. A raw {@code BytesRef} is refused for those
     * types, because it is exactly the shape that cannot be checked. {@code KEYWORD}, {@code TEXT} and
     * {@code SOURCE} take a {@code String} or a {@code BytesRef}, since for them the bytes <i>are</i> the text.
     * <p>
     * A {@code List} is a multivalued cell, which the builders support, so each element goes through the same rule.
     */
    private static Object encodedFor(Attribute column, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> multivalue) {
            List<Object> encoded = new ArrayList<>(multivalue.size());
            for (Object element : multivalue) {
                encoded.add(encodedFor(column, element));
            }
            return encoded;
        }
        DataType declared = column.dataType();
        if (PlannerUtils.toElementType(declared) != ElementType.BYTES_REF) {
            // The numeric and boolean builders cast, so a wrong type fails there with the column named by page.
            return value;
        }
        return switch (declared) {
            case KEYWORD, TEXT, SOURCE -> value instanceof String || value instanceof BytesRef
                ? value
                : refuse(column, value, "a String or a BytesRef");
            case IP -> switch (value) {
                case InetAddress address -> new BytesRef(InetAddressPoint.encode(address));
                // The one encoded family with a cheap invariant, so an already-encoded value is still accepted.
                case BytesRef bytes when bytes.length == InetAddressPoint.BYTES -> bytes;
                default -> refuse(column, value, "an InetAddress");
            };
            case VERSION -> switch (value) {
                case Version version -> version.toBytesRef();
                case String text -> new Version(text).toBytesRef();
                default -> refuse(column, value, "a Version or a String");
            };
            case GEO_POINT, GEO_SHAPE -> value instanceof Geometry geometry
                ? SpatialCoordinateTypes.GEO.asWkb(geometry)
                : refuse(column, value, "a Geometry");
            case CARTESIAN_POINT, CARTESIAN_SHAPE -> value instanceof Geometry geometry
                ? SpatialCoordinateTypes.CARTESIAN.asWkb(geometry)
                : refuse(column, value, "a Geometry");
            // No natural Java form to encode from, so the function owns the bytes and says so by handing them over.
            default -> value instanceof BytesRef ? value : refuse(column, value, "a BytesRef");
        };
    }

    private static Object refuse(Attribute column, Object value, String wanted) {
        throw new IllegalStateException(
            "table function produced ["
                + value.getClass().getSimpleName()
                + "] for column ["
                + column.name()
                + "] declared ["
                + column.dataType().typeName()
                + "], which needs "
                + wanted
        );
    }

    /**
     * The declared columns are the only source of each block's type.
     * <p>
     * {@code BlockUtils.fromList} would infer an element type per column from the first non-null value it found,
     * which is a second derivation of what {@link #columns} already states: declare {@code LONG}, hand back an
     * {@code Integer}, and the page carries an {@code INT} block under a {@code LONG} attribute with nothing
     * between the two to notice. A zero-row answer still owes one empty block per column for the same reason,
     * which is why there is no special case for it here.
     * <p>
     * Typing the builder from the declaration is not by itself enough to catch a value of the wrong Java type —
     * see {@link #encodedFor}, which owns the encoding for the family whose bytes are not their text.
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
                    Object value = encodedFor(columns.get(i), row.get(i));
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
