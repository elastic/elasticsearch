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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;
import org.elasticsearch.xpack.versionfield.Version;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
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
 * length of the query and a function's by whatever it decided to return — so <b>a function states its own bound
 * through {@link #maxRows()}</b>, which has no default and which {@link #page} enforces, and one whose row count
 * has no bound it can state does not belong on this base class.
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
            listener.map(rows -> new LocalRelation(call.source(), columns, LocalSupplier.of(page(call.name(), maxRows(), columns, rows))))
        );
    }

    /**
     * The most rows this function will hand back — stated by the function, with no default, because the base class
     * cannot know and guessing a cap for someone else's relation is worse than making them say.
     * <p>
     * This is the class javadoc's rule made into a signature. Nothing caps the row count downstream and no circuit
     * breaker is charged, so the bound was a sentence a function's author had to read and obey; now it is a method
     * they cannot compile without, and {@link #page} refuses past it before a single block is built. A function
     * whose row count has no bound it can state does not belong on this base class — that is the same sentence,
     * and now there is nowhere to put the number it does not have.
     */
    protected abstract int maxRows();

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
            // list under a BYTES_REF column is stored as its own toString(); a null element reaches
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
            return encodedNumber(function, column, declared, value);
        }
        return switch (declared) {
            case KEYWORD, TEXT -> value instanceof String || value instanceof BytesRef
                ? value
                : refuse(function, column, value, "a String or a BytesRef");
            // Two readers disagree about a flattened cell: ResponseValueUtils takes utf8ToString, PositionToXContent
            // parses the bytes as XContent for the JSON response. The strict one decides, or text that is not JSON
            // is accepted here and throws NotXContentException while the response is written.
            case FLATTENED -> readBack(function, column, bytesOf(function, column, value), CoordinatorRowsFunction::parseJson);
            // A raw BytesRef is refused here as it is everywhere else encoded. The sixteen-byte length looked like
            // an invariant and is not one: every 16-character IPv6 literal is sixteen bytes of text, so
            // BytesRef("2001:db8::ff00:4") passed and decoded as a different address. A function holding encoded
            // bytes has InetAddressPoint.decode.
            case IP -> value instanceof InetAddress address
                ? new BytesRef(InetAddressPoint.encode(address))
                : refuse(function, column, value, "an InetAddress");
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
            // Enumerated rather than left to a default arm: a default standing for valid types is what made the
            // encoded types above arrive one at a time, each silently taking the raw-bytes pass-through until
            // somebody noticed. A BYTES_REF type added later lands on the throw instead.
            case UNSUPPORTED, TSID_DATA_TYPE -> value instanceof BytesRef ? value : refuse(function, column, value, "a BytesRef");
            // Bytes the function owns, but with a reader that can say whether they are what the type claims, so
            // they are read back the way encodedPoint and encodedShape read theirs back. Without this a histogram
            // or a _source cell of arbitrary bytes is accepted here and throws in the response writer instead -
            // a function's bug reported at result time, which is what this seam exists to prevent.
            case HISTOGRAM -> readBack(function, column, value, bytes -> EsqlDataTypeConverter.histogramToString(bytes));
            case SOURCE -> readBack(function, column, value, CoordinatorRowsFunction::parseSource);
            default -> throw badCell(
                function,
                column,
                "no cell encoding is declared for this type - add an arm to CoordinatorRowsFunction"
            );
        };
    }

    /**
     * A cell for a column the engine reads out of a numeric or boolean block, where the Java type is not the whole
     * question either: several of these longs are an <i>encoding</i> rather than the number written, and the cast
     * {@code BlockUtils.appendValue} performs cannot tell the difference.
     * <p>
     * {@code unsigned_long} is stored biased — the number plus {@code Long.MIN_VALUE} — so a {@code Long} of 5 is
     * read back as 9223372036854775813. The unambiguous natural form is a {@code BigInteger}, so that is what this
     * takes and encodes, range-checked first because {@code NumericUtils.asLongUnsigned} says it checks nothing. A
     * {@code Long} is refused rather than guessed at: nothing distinguishes one meant as the number from one
     * already biased, and either reading is silently wrong for the other caller.
     * <p>
     * {@code geohash}, {@code geotile} and {@code geohex} hold a grid id, and the engine has a validator for each
     * ({@code EsqlDataTypeConverter.longToGeohash}, {@code longToGeotile}, {@code longToGeohex}) plus a parser for
     * each natural string form, so both are used. They are checked rather than trusted because an invalid grid long
     * is accepted by every builder and then throws in the response writer, which turns a function's bug into an
     * error at result time — the thing {@link #encodedPoint} refuses for the same reason.
     * <p>
     * {@code datetime} and {@code date_nanos} hold the number written, in a unit the declaration decides: millis
     * for one and nanos since the epoch for the other, so the same {@code Long} means two different instants and
     * only the column says which. An {@link Instant} is the form that cannot be misread, and it is accepted and
     * encoded per declared type; a {@code Long} is taken as already being in the column's unit. A negative
     * {@code date_nanos} is refused, because nothing downstream can hold one: {@code ToDateNanos.fromLong} and
     * {@code DateUtils.toNanoSeconds} both refuse pre-epoch, so no index and no conversion produces one. The
     * {@code Long} form is checked here and the {@code Instant} form by {@code DateUtils.toLong}, which is the
     * call this makes and which refuses the far end of the range too.
     * <p>
     * Everything else — {@code long}, {@code integer}, {@code double}, {@code boolean}, the counters — is the
     * number written, and passes through. {@link #ENCODED_LONGS} is what a test holds the whole set of
     * {@code ElementType.LONG} types against, so a new type whose long is an encoding is a red test rather than a
     * value that passes through here looking like the number it is not.
     */
    private static Object encodedNumber(String function, Attribute column, DataType declared, Object value) {
        return switch (declared) {
            case UNSIGNED_LONG -> {
                if (value instanceof BigInteger natural) {
                    if (NumericUtils.isUnsignedLong(natural) == false) {
                        throw badCell(function, column, "[" + natural + "] is outside the unsigned_long range");
                    }
                    yield NumericUtils.asLongUnsigned(natural);
                }
                yield refuse(function, column, value, "a BigInteger");
            }
            case GEOHASH, GEOTILE, GEOHEX -> encodedGrid(function, column, declared, value);
            case DATETIME -> value instanceof Instant instant ? instant.toEpochMilli() : value;
            case DATE_NANOS -> {
                if (value instanceof Instant instant) {
                    try {
                        // Wrapped like every other converter call here: DateUtils.toLong refuses pre-epoch and
                        // anything past 2262-04-11T23:47:16.854775807Z with an IllegalArgumentException, which
                        // unwrapped would name neither the function nor the column and would reach the user as a
                        // 400 - their fault - for a bug in the function.
                        yield DateUtils.toLong(instant);
                    } catch (IllegalArgumentException e) {
                        throw badCell(function, column, e.getMessage(), e);
                    }
                }
                if (value instanceof Long nanos && nanos < 0) {
                    throw badCell(function, column, "[" + nanos + "] is before 1970-01-01T00:00:00Z, which date_nanos cannot hold");
                }
                yield value;
            }
            // The numeric and boolean builders cast, so a wrong type fails there with the column named by page.
            default -> value;
        };
    }

    /**
     * Bytes checked by handing them to the reader that will read them, so the rule is the reader's rather than a
     * restatement of it here. The value read back is discarded: what is being asked is whether reading throws.
     */
    private static Object readBack(String function, Attribute column, Object value, Consumer<BytesRef> reader) {
        if (value instanceof BytesRef bytes) {
            try {
                reader.accept(bytes);
            } catch (Exception e) {
                throw badCell(function, column, "the bytes are not a readable " + column.dataType().typeName(), e);
            }
            return bytes;
        }
        return refuse(function, column, value, "a BytesRef");
    }

    /** A {@code String} is still fine for a text-shaped column; the bytes are what gets checked. */
    private static Object bytesOf(String function, Attribute column, Object value) {
        if (value instanceof String text) {
            return new BytesRef(text);
        }
        if (value instanceof BytesRef bytes) {
            return bytes;
        }
        return refuse(function, column, value, "a String or a BytesRef");
    }

    /** Any JSON value, not only an object, which is what a flattened cell may hold. */
    private static void parseJson(BytesRef bytes) {
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                new BytesArray(bytes.bytes, bytes.offset, bytes.length)
            )
        ) {
            parser.nextToken();
            parser.skipChildren();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Parses a {@code _source} cell as the response writer parses one, for its side effect of throwing. */
    private static void parseSource(BytesRef bytes) {
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                new BytesArray(bytes.bytes, bytes.offset, bytes.length)
            )
        ) {
            parser.nextToken();
            parser.mapOrdered();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * A grid cell, from the string form a user would write or the long the engine stores, both put through the
     * engine's own converter so neither rule is restated here to drift from it.
     */
    private static Object encodedGrid(String function, Attribute column, DataType declared, Object value) {
        try {
            if (value instanceof String text) {
                return switch (declared) {
                    case GEOHASH -> EsqlDataTypeConverter.stringToGeohash(text);
                    case GEOTILE -> EsqlDataTypeConverter.stringToGeotile(text);
                    case GEOHEX -> EsqlDataTypeConverter.stringToGeohex(text);
                    default -> throw new AssertionError("not a grid type [" + declared + "]");
                };
            }
            if (value instanceof Long encoded) {
                return switch (declared) {
                    case GEOHASH -> EsqlDataTypeConverter.longToGeohash(encoded);
                    case GEOTILE -> EsqlDataTypeConverter.longToGeotile(encoded);
                    case GEOHEX -> EsqlDataTypeConverter.longToGeohex(encoded);
                    default -> throw new AssertionError("not a grid type [" + declared + "]");
                };
            }
        } catch (IllegalArgumentException e) {
            throw badCell(function, column, e.getMessage(), e);
        }
        return refuse(function, column, value, "a String or a Long");
    }

    /**
     * The types whose {@code long} this seam encodes or checks, named so that a test can hold the set of
     * {@code ElementType.LONG} types against it: a new one is a red test rather than a value that passes through
     * {@link #encodedNumber} looking like the number it is not. Package-private for
     * {@code TableFunctionResolverTests.testEveryLongBackedTypeIsEitherEncodedHereOrPlainlyTheNumber} alone.
     */
    static final Set<DataType> ENCODED_LONGS = Set.of(
        DataType.UNSIGNED_LONG,
        DataType.GEOHASH,
        DataType.GEOTILE,
        DataType.GEOHEX,
        DataType.DATE_NANOS
    );

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
                throw badCell(function, column, e.getMessage(), e);
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
                throw badCell(function, column, e.getMessage(), e);
            }
            return wkb;
        }
        throw wrongType(function, column, value, "a Geometry");
    }

    /** A cell that is the wrong shape rather than the wrong Java type, so there is no "needs" to name. */
    private static IllegalStateException badCell(String function, Attribute column, String why) {
        return badCell(function, column, why, null);
    }

    /** As above, keeping the exception that decided it: the message is carried verbatim, the frame below is not. */
    private static IllegalStateException badCell(String function, Attribute column, String why, @Nullable Throwable cause) {
        return new IllegalStateException(
            "table function ["
                + function
                + "] produced an invalid cell for column ["
                + column.name()
                + "] declared ["
                + column.dataType().typeName()
                + "]: "
                + why,
            cause
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
    private static Page page(String function, int maxRows, List<Attribute> columns, List<List<Object>> rows) {
        if (rows == null) {
            throw new IllegalStateException("table function [" + function + "] produced no row list at all");
        }
        // A bound no row could satisfy is a declaration bug, not a refusal to report per call, and it is cheaper
        // to find here than to wonder why a function never returns anything.
        if (maxRows < 1) {
            throw new IllegalStateException("table function [" + function + "] declared a row bound of [" + maxRows + "]");
        }
        // Before any block is built, so a function that produced far more than it said costs one list rather than
        // a page of it. Nothing downstream would refuse it: the page is already materialized by then.
        if (rows.size() > maxRows) {
            throw new IllegalStateException(
                "table function ["
                    + function
                    + "] produced ["
                    + rows.size()
                    + "] rows, past the ["
                    + maxRows
                    + "] it declared it would not exceed"
            );
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
            boolean success = false;
            try {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = wrappers[i].builder().build();
                }
                Page page = new Page(rows.size(), blocks);
                success = true;
                return page;
            } finally {
                // Closing the wrappers does not reach a block already built, so a throw from a later build() or
                // from the Page constructor would leave the earlier ones held. BlockUtils.fromList and
                // fromListRow - the methods this one displaces - each guard the same loop the same way.
                if (success == false) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        } finally {
            Releasables.close(wrappers);
        }
    }
}
