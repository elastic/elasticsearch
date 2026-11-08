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
import org.elasticsearch.geometry.Point;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
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
 * <h2>What a function's rows cost</h2>
 *
 * All of them, in memory, for as long as the plan lives. A subclass hands over a {@code List<List<Object>>} of
 * boxed values, this builds one block per column through {@code PlannerUtils.NON_BREAKING_BLOCK_FACTORY}, and the
 * resulting {@link Page} sits in the {@code LocalRelation}. Nothing here caps the row count and no circuit
 * breaker is charged. That is the same arrangement {@code ROW} has, but {@code ROW}'s size is bounded by the
 * length of the query and a function's by whatever it decided to return — so
 * <b>a function that cannot say how many rows it will produce has to bound itself</b>, and one that lists
 * something unbounded does not belong on this base class.
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
        rows(
            call,
            context,
            listener.map(rows -> new LocalRelation(call.source(), columns, LocalSupplier.of(page(call.name(), columns, rows))))
        );
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
     * types wherever nothing about it can be checked, which is all of them but {@code ip}, whose encoding is a
     * fixed sixteen bytes and so carries its own invariant. {@code KEYWORD}, {@code TEXT} and {@code SOURCE} take
     * a {@code String} or a {@code BytesRef}, since for them the bytes <i>are</i> the text.
     * <p>
     * A {@code List} is a multivalued cell, and each element goes through the same rule — but one level only, with
     * no null element and never empty, because a block position holds values and not cells.
     */
    private static Object encodedFor(String function, Attribute column, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> multivalue) {
            // One level deep, no nulls and never empty, because a block position holds values rather than cells.
            // BuilderWrapper opens a position entry and hands each element to BlockUtils.appendValue, so: a nested
            // list reaches the BYTES_REF arm and is stored as its own toString(); a null element reaches
            // appendNull, which closes the open entry and starts another, turning one row into three positions in
            // a page that says it has one; and an empty list opens an entry and closes it with nothing in it. The
            // block API guards all three with assertions alone, so without this they are production-only faults.
            if (multivalue.isEmpty()) {
                throw badCell(function, column, "an empty list has no value to store - hand back null for an empty cell");
            }
            List<Object> encoded = new ArrayList<>(multivalue.size());
            for (Object element : multivalue) {
                if (element == null) {
                    throw badCell(function, column, "a multivalued cell cannot hold null - hand back null for the whole cell");
                }
                if (element instanceof List<?>) {
                    throw badCell(function, column, "a multivalued cell cannot hold a list - a position holds values, not cells");
                }
                encoded.add(encodedFor(function, column, element));
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
                : refuse(function, column, value, "a String or a BytesRef");
            case IP -> switch (value) {
                case InetAddress address -> new BytesRef(InetAddressPoint.encode(address));
                // The one encoded family with a cheap invariant, so an already-encoded value is still accepted.
                case BytesRef bytes when bytes.length == InetAddressPoint.BYTES -> bytes;
                default -> refuse(function, column, value, "an InetAddress");
            };
            case VERSION -> switch (value) {
                case Version version -> version.toBytesRef();
                case String text -> new Version(text).toBytesRef();
                default -> refuse(function, column, value, "a Version or a String");
            };
            case GEO_POINT -> encodedPoint(function, column, SpatialCoordinateTypes.GEO, value);
            case CARTESIAN_POINT -> encodedPoint(function, column, SpatialCoordinateTypes.CARTESIAN, value);
            case GEO_SHAPE -> encodedShape(function, column, SpatialCoordinateTypes.GEO, value);
            case CARTESIAN_SHAPE -> encodedShape(function, column, SpatialCoordinateTypes.CARTESIAN, value);
            // No natural Java form to encode from, so the function owns the bytes and says so by handing them over.
            default -> value instanceof BytesRef ? value : refuse(function, column, value, "a BytesRef");
        };
    }

    /**
     * A point column holds a point, and one the coordinate system admits. {@code asWkb} writes whatever geometry it
     * is handed with no dimensionality or coordinate check, so a polygon would sit under a point column as 93 bytes
     * of polygon, and a latitude of 500 would sit there as a point — each failing far downstream, in whatever reads
     * it. Validated by reading the bytes back the way the engine will: {@code wkbAsPoint} applies this coordinate
     * system's own validator and refuses a non-point, so neither rule is restated here to drift from it.
     */
    private static BytesRef encodedPoint(String function, Attribute column, SpatialCoordinateTypes system, Object value) {
        if (value instanceof Point point) {
            BytesRef wkb = system.asWkb(point);
            try {
                system.wkbAsPoint(wkb);
            } catch (IllegalArgumentException e) {
                throw badCell(function, column, e.getMessage());
            }
            return wkb;
        }
        throw wrongType(function, column, value, "a Point");
    }

    /** As {@link #encodedPoint}, for the shape types, which admit any geometry whose coordinates are in range. */
    private static BytesRef encodedShape(String function, Attribute column, SpatialCoordinateTypes system, Object value) {
        if (value instanceof Geometry geometry) {
            BytesRef wkb = system.asWkb(geometry);
            try {
                system.wkbToGeometry(wkb);
            } catch (IllegalArgumentException e) {
                throw badCell(function, column, e.getMessage());
            }
            return wkb;
        }
        throw wrongType(function, column, value, "a Geometry");
    }

    /** A cell that is the wrong shape rather than the wrong Java type, so there is no "needs" to name. */
    private static IllegalStateException badCell(String function, Attribute column, String why) {
        return new IllegalStateException(
            "table function ["
                + function
                + "] produced an invalid cell for column ["
                + column.name()
                + "] declared ["
                + column.dataType().typeName()
                + "]: "
                + why
        );
    }

    /** Throws, and returns {@code Object} so a switch arm producing a value can use it as one. */
    private static Object refuse(String function, Attribute column, Object value, String wanted) {
        throw wrongType(function, column, value, wanted);
    }

    private static IllegalStateException wrongType(String function, Attribute column, Object value, String wanted) {
        return new IllegalStateException(producedFor(function, column, value) + ", which needs " + wanted);
    }

    private static String producedFor(String function, Attribute column, Object value) {
        return "table function ["
            + function
            + "] produced "
            + describe(value)
            + " for column ["
            + column.name()
            + "] declared ["
            + column.dataType().typeName()
            + "]";
    }

    /**
     * What the cell held, rather than which class the JDK happened to use for it. A list reported as its own class
     * name reads as {@code List12}, which names neither that it was a list nor what was in it.
     */
    private static String describe(Object value) {
        if (value == null) {
            return "[null]";
        }
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                return "an empty list";
            }
            Set<String> elements = new LinkedHashSet<>();
            for (Object element : list) {
                elements.add(element == null ? "null" : element.getClass().getSimpleName());
            }
            return "a list of [" + String.join(", ", elements) + "]";
        }
        return "[" + value.getClass().getSimpleName() + "]";
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
    private static Page page(String function, List<Attribute> columns, List<List<Object>> rows) {
        if (rows == null) {
            throw new IllegalStateException("table function [" + function + "] produced no row list at all");
        }
        // A relation with no output, or with one name twice, is a plan the rest of the language has no answer for:
        // a reference to the repeated name is ambiguous and nothing downstream is positioned to say which
        // function caused it. Refused here because this is where the columns and the function are both in hand.
        if (columns.isEmpty()) {
            throw new IllegalStateException("table function [" + function + "] declared no columns");
        }
        Set<String> names = new LinkedHashSet<>(columns.size());
        for (Attribute column : columns) {
            if (names.add(column.name()) == false) {
                throw new IllegalStateException(
                    "table function [" + function + "] declared the column [" + column.name() + "] more than once"
                );
            }
        }
        BlockUtils.BuilderWrapper[] wrappers = new BlockUtils.BuilderWrapper[columns.size()];
        try {
            for (int i = 0; i < wrappers.length; i++) {
                Attribute column = columns.get(i);
                try {
                    ElementType type = PlannerUtils.toElementType(column.dataType());
                    wrappers[i] = BlockUtils.wrapperFor(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, type, rows.size());
                } catch (Exception e) {
                    // A declared type with no element type at all fails here, in toElementType, and the raw failure
                    // names the element type rather than the column that asked for it - which is the only thing a
                    // function's author can change. A type that has an element type but no way to append a value,
                    // ElementType.NULL being the one, gets as far as a builder and fails at the append below.
                    throw new IllegalStateException(
                        "table function ["
                            + function
                            + "] declared column ["
                            + column.name()
                            + "] as ["
                            + column.dataType().typeName()
                            + "], which it cannot produce values for",
                        e
                    );
                }
            }
            for (int r = 0; r < rows.size(); r++) {
                List<Object> row = rows.get(r);
                // Checked rather than asserted: a function that disagrees with its own column list would otherwise
                // build a page of a different width than the output it is attached to, and do it in production only.
                if (row == null) {
                    throw new IllegalStateException("table function [" + function + "] produced a null row at [" + r + "]");
                }
                if (row.size() != wrappers.length) {
                    throw new IllegalStateException(
                        "table function ["
                            + function
                            + "] produced a row of ["
                            + row.size()
                            + "] values at ["
                            + r
                            + "] for ["
                            + wrappers.length
                            + "] columns"
                    );
                }
                for (int i = 0; i < wrappers.length; i++) {
                    Object value = encodedFor(function, columns.get(i), row.get(i));
                    try {
                        wrappers[i].accept(value);
                    } catch (ClassCastException | UnsupportedOperationException e) {
                        // Two ways one value fails to go into one column, both of them reported against the element
                        // type alone. A builder typed from the declaration casts, so a value of another Java type is
                        // a ClassCastException; a declared type whose element type has no append arm at all -
                        // ElementType.NULL - is an UnsupportedOperationException, and fails for every value. Neither
                        // names the column or the declaration it disagreed with, which is what this adds.
                        throw new IllegalStateException(producedFor(function, columns.get(i), value), e);
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
