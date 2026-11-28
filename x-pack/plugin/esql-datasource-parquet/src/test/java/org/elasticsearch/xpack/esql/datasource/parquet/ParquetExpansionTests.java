/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.datasource.parquet;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandedRows;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SharedErrorBudget;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/**
 * The expanded rows a Parquet read emits for a list of structures, on the optimized iterator and on the baseline one,
 * which must agree. Every element carries its parent's id in a {@code pid} leaf where a test needs to prove that an
 * element row sits beside its own parent's values.
 */
public class ParquetExpansionTests extends ESTestCase {

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("""
        message t {
          required int64 id;
          optional binary label (UTF8);
          optional int64 n;
          optional group items (LIST) {
            repeated group list {
              optional group element {
                optional int64 pid;
                optional binary sku (UTF8);
                optional int32 qty;
                optional double price;
                optional group tags (LIST) { repeated group list { optional binary element (UTF8); } }
              }
            }
          }
        }
        """);

    /** The legacy two-level list of the Parquet backward-compatibility rules: the repeated group is the element. */
    private static final MessageType TWO_LEVEL = MessageTypeParser.parseMessageType("""
        message t {
          required int64 id;
          optional group items (LIST) {
            repeated group element {
              optional int64 pid;
              optional binary sku (UTF8);
            }
          }
        }
        """);

    /** A required list of required elements, so the levels that make an element exist and not null coincide. */
    private static final MessageType REQUIRED = MessageTypeParser.parseMessageType("""
        message t {
          required int64 id;
          required group items (LIST) {
            repeated group list {
              required group element {
                required int64 pid;
                required group dim {
                  required binary sku (UTF8);
                }
              }
            }
          }
        }
        """);

    /** A leaf for each of the value conversions {@link #testValueKinds} reads at their edges. */
    private static final MessageType KINDS = MessageTypeParser.parseMessageType("""
        message t {
          required int64 id;
          optional group items (LIST) {
            repeated group list {
              optional group element {
                optional boolean b;
                optional int32 d (DATE);
                optional int32 u (INTEGER(32, false));
                optional int64 ts (TIMESTAMP(MICROS, true));
                optional float f;
                optional double x;
              }
            }
          }
        }
        """);

    private final boolean optimized;
    private BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public ParquetExpansionTests(@Name("optimized") boolean optimized) {
        this.optimized = optimized;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    /** The Parquet reader honours an expand read on both iterators and both read paths, and says so. */
    public void testReaderSupportsExpandRead() {
        assertTrue(reader().supportsExpandRead());
    }

    /**
     * A file whose element holds more fields than the reader's field cap is refused by the read, with the reason discovery
     * gives the same collection: the read describes the element under the cap discovery uses. The same file reads under
     * the default cap.
     */
    public void testElementWiderThanTheFieldCapRefused() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            r0.addGroup("items").addGroup("list").addGroup("element").append("pid", 0L);
            return List.of(r0);
        });
        // Four columns fit the cap; the element's five fields do not
        Exception e = expectThrows(
            Exception.class,
            () -> read(data, context(List.of("id"), leaf("pid", DataType.LONG)), r -> r.withSchemaMaxFields(4))
        );
        assertThat(causeMessages(e), containsString("[items] holds more than [4] fields in each element, which cannot be read"));
        assertThat(read(data, context(List.of("id"), leaf("pid", DataType.LONG))).rows, equalTo(List.of(List.of(0L, 0L))));
    }

    /**
     * Two parents with two elements each: each parent's values repeat on its element rows, and the element columns
     * follow the parent columns, line up element by element, and keep a list inside the element as a multivalue.
     */
    public void testParentsRepeatPerElement() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L).append("label", "zero");
            Group items0 = r0.addGroup("items");
            Group e0 = items0.addGroup("list").addGroup("element").append("pid", 0L).append("sku", "a");
            e0.addGroup("tags").addGroup("list").append("element", "x");
            Group e1 = items0.addGroup("list").addGroup("element").append("pid", 0L).append("sku", "b");
            Group tags1 = e1.addGroup("tags");
            tags1.addGroup("list").append("element", "y");
            tags1.addGroup("list").append("element", "z");
            Group r1 = factory.newGroup().append("id", 1L).append("label", "one");
            Group items1 = r1.addGroup("items");
            items1.addGroup("list").addGroup("element").append("pid", 1L).append("sku", "c");
            items1.addGroup("list").addGroup("element").append("pid", 1L).append("sku", "d");
            return List.of(r0, r1);
        });
        Expanded read = read(
            data,
            context(List.of("id", "label"), leaf("pid", DataType.LONG), leaf("sku", DataType.KEYWORD), leaf("tags", DataType.KEYWORD))
        );
        assertThat(
            read.rows,
            equalTo(
                List.of(
                    Arrays.asList(0L, ref("zero"), 0L, ref("a"), ref("x")),
                    Arrays.asList(0L, ref("zero"), 0L, ref("b"), List.of(ref("y"), ref("z"))),
                    Arrays.asList(1L, ref("one"), 1L, ref("c"), null),
                    Arrays.asList(1L, ref("one"), 1L, ref("d"), null)
                )
            )
        );
        assertThat(read.blockCounts, equalTo(List.of(5)));
    }

    /** A null field of an element is null in that element's row and nowhere else. */
    public void testNullElementFieldIsNullInItsRow() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("sku", "a").append("qty", 1);
            items.addGroup("list").addGroup("element").append("qty", 2);
            items.addGroup("list").addGroup("element").append("sku", "c");
            return List.of(r0);
        });
        Expanded read = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD), leaf("qty", DataType.INTEGER)));
        assertThat(
            read.rows,
            equalTo(List.of(Arrays.asList(0L, ref("a"), 1), Arrays.asList(0L, null, 2), Arrays.asList(0L, ref("c"), null)))
        );
    }

    /** An empty list and a null list each keep their parent as exactly one row whose element columns are null. */
    public void testEmptyAndNullListGiveOneNullRow() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            r0.addGroup("items");
            Group r1 = factory.newGroup().append("id", 1L);
            Group r2 = factory.newGroup().append("id", 2L);
            r2.addGroup("items").addGroup("list").addGroup("element").append("sku", "z");
            return List.of(r0, r1, r2);
        });
        Expanded read = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD), leaf("qty", DataType.INTEGER)));
        assertThat(
            read.rows,
            equalTo(List.of(Arrays.asList(0L, null, null), Arrays.asList(1L, null, null), Arrays.asList(2L, ref("z"), null)))
        );
    }

    /**
     * A null value of a list inside the element is dropped and announced as a list column announces its own, and a batch
     * whose read fails announces nothing.
     */
    public void testNullValueOfAnInnerListIsAnnounced() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group tags = r0.addGroup("items").addGroup("list").addGroup("element").append("sku", "a").addGroup("tags");
            tags.addGroup("list").append("element", "x");
            tags.addGroup("list");
            return List.of(r0);
        });
        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        Expanded read = read(data, context(List.of("id"), leaf("tags", DataType.KEYWORD)).informationalWarningSink(warnings::add));
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, ref("x")))));
        assertThat(warnings, hasItem(containsString(ParquetColumnDecoding.nullListElementsMessage("items.tags"))));
        assertThat(warnings, not(hasItem(containsString(ParquetColumnDecoding.nullListElementsMessage("items")))));
    }

    /**
     * A null element is dropped and announced with the warning a list column's null elements raise, and a parent whose
     * only element is null has no elements, so it keeps one null row. That is the index side's behaviour: a null in a
     * nested array is not indexed as a nested object, and {@code NestedExpandOperator} keeps one null row for a parent
     * without nested objects.
     */
    public void testNullElementDroppedAndOnlyNullElementGivesNullRow() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items0 = r0.addGroup("items");
            items0.addGroup("list").addGroup("element").append("sku", "a");
            items0.addGroup("list");
            items0.addGroup("list").addGroup("element").append("sku", "c");
            Group r1 = factory.newGroup().append("id", 1L);
            r1.addGroup("items").addGroup("list");
            return List.of(r0, r1);
        });
        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        Expanded read = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD)).informationalWarningSink(warnings::add));
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, ref("a")), Arrays.asList(0L, ref("c")), Arrays.asList(1L, null))));
        assertThat(warnings, hasItem(containsString(ParquetColumnDecoding.nullListElementsMessage("items"))));
    }

    /**
     * A leaf the file lacks is null on every element row, beside a leaf it has; asked for alone, the elements are still
     * there, found from another leaf's levels.
     */
    public void testMissingLeafIsNull() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("sku", "a");
            items.addGroup("list").addGroup("element").append("qty", 3);
            return List.of(r0, factory.newGroup().append("id", 1L));
        });
        Expanded both = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD), leaf("color", DataType.KEYWORD)));
        assertThat(
            both.rows,
            equalTo(List.of(Arrays.asList(0L, ref("a"), null), Arrays.asList(0L, null, null), Arrays.asList(1L, null, null)))
        );
        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        Expanded alone = read(data, context(List.of("id"), leaf("color", DataType.KEYWORD)).informationalWarningSink(warnings::add));
        assertThat(alone.rows, equalTo(List.of(Arrays.asList(0L, null), Arrays.asList(0L, null), Arrays.asList(1L, null))));
        // Announced as an absent column is
        assertThat(warnings, equalTo(List.of(SkipWarnings.absentColumnMessage("items.color"))));
    }

    /** A file without the collection keeps every parent as one null row. */
    public void testFileWithoutCollection() throws Exception {
        MessageType flat = MessageTypeParser.parseMessageType("message t { required int64 id; }");
        byte[] data = write(flat, factory -> List.of(factory.newGroup().append("id", 0L), factory.newGroup().append("id", 1L)));
        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        Expanded read = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD)).informationalWarningSink(warnings::add));
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, null), Arrays.asList(1L, null))));
        // Announced as an absent column is
        assertThat(warnings, equalTo(List.of(SkipWarnings.absentColumnMessage("items"))));
    }

    /** A leaf the file stores as another type is converted to the requested one when that conversion exists. */
    public void testCastLeaf() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("qty", 7);
            items.addGroup("list").addGroup("element").append("pid", 0L);
            return List.of(r0);
        });
        Expanded read = read(data, context(List.of("id"), leaf("qty", DataType.LONG)));
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, 7L), Arrays.asList(0L, null))));
    }

    /**
     * A leaf's type is inferred, so a file storing a type that does not widen to it reads the leaf as null, with a
     * warning, under every error policy, as an inferred column does ({@code validatePlannerTypesAgainstFile}). Neither a
     * narrowing (a long read as an integer) nor an unrelated type (a long read as a boolean) fails the read or drops its
     * rows, and the leaves the file can supply still read.
     */
    public void testLeafThatDoesNotWidenReadsAsNullUnderEveryPolicy() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("pid", 1L << 40).append("sku", "a");
            items.addGroup("list").addGroup("element").append("pid", 5L).append("sku", "b");
            return List.of(r0, factory.newGroup().append("id", 1L));
        });
        for (ErrorPolicy policy : List.of(
            ErrorPolicy.STRICT,
            ErrorPolicy.PERMISSIVE,
            new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 0, 0.0, false)
        )) {
            for (DataType type : List.of(DataType.INTEGER, DataType.BOOLEAN)) {
                List<String> warnings = new ArrayList<>();
                Expanded read = read(
                    data,
                    context(List.of("id"), leaf("pid", type), leaf("sku", DataType.KEYWORD)).errorPolicy(policy)
                        .informationalWarningSink(warnings::add)
                );
                String what = policy.mode() + " " + type;
                assertThat(
                    what,
                    read.rows,
                    equalTo(List.of(Arrays.asList(0L, null, ref("a")), Arrays.asList(0L, null, ref("b")), Arrays.asList(1L, null, null)))
                );
                assertThat(
                    what,
                    warnings,
                    hasItem(containsString("column [items.pid]: [long] in the file, [" + type.typeName() + "] in the query"))
                );
            }
        }
    }

    /**
     * Under {@code skip_row}, an element value the widening cast cannot represent drops its parent, every row of it,
     * as a column value drops its row, and charges the read's budget once for that parent; {@code null_field} nulls the
     * value and keeps the row; {@code fail_fast} fails the read. A millisecond timestamp past 2262 has no nanosecond value.
     */
    public void testElementCastFailureDropsTheParentUnderSkipRow() throws Exception {
        MessageType millis = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    optional int64 at (TIMESTAMP(MILLIS, true));
                  }
                }
              }
            }
            """);
        long year3000 = 32_503_680_000_000L;
        byte[] data = write(millis, factory -> {
            List<Group> groups = new ArrayList<>();
            for (long r = 0; r < 4; r++) {
                Group g = factory.newGroup().append("id", r);
                Group items = g.addGroup("items");
                items.addGroup("list").addGroup("element").append("at", 1_000L);
                items.addGroup("list").addGroup("element").append("at", r == 1 ? year3000 : 2_000L);
                items.addGroup("list").addGroup("element").append("at", r == 1 ? year3000 : 3_000L);
                groups.add(g);
            }
            return groups;
        });
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, Long.MAX_VALUE, 1.0, false);
        SharedErrorBudget budget = SharedErrorBudget.forPolicy(skipRow, "memory://expand.parquet");
        List<String> warnings = new ArrayList<>();
        Expanded dropped = read(
            data,
            context(List.of("id"), leaf("at", DataType.DATE_NANOS)).errorPolicy(skipRow)
                .sharedErrorBudget(budget)
                .informationalWarningSink(warnings::add)
        );
        assertThat(dropped.rows.stream().map(row -> row.get(0)).distinct().toList(), equalTo(List.of(0L, 2L, 3L)));
        assertThat(dropped.rows.size(), equalTo(9));
        assertThat("one parent, one error", budget.errorCount(), equalTo(1L));
        assertThat(warnings, hasItem(containsString("cannot be read as their merged type; skipping their rows")));

        Expanded nulled = read(
            data,
            context(List.of("id"), leaf("at", DataType.DATE_NANOS)).errorPolicy(ErrorPolicy.PERMISSIVE).informationalWarningSink(w -> {})
        );
        assertThat(nulled.rows.size(), equalTo(12));
        assertThat(nulled.rows.get(4).get(1), equalTo(null));
        assertThat(nulled.rows.get(3).get(1), equalTo(1_000_000_000L));

        Exception e = expectThrows(
            Exception.class,
            () -> read(data, context(List.of("id"), leaf("at", DataType.DATE_NANOS)).errorPolicy(ErrorPolicy.STRICT))
        );
        assertThat(causeMessages(e), containsString("column [items.at]: cannot read [datetime] as [date_nanos]"));
    }

    /**
     * The parents an element value drops count against the read's one error budget, with the file's rows as the rows: a
     * ratio is enforced even with no budget shared with the operator, where the reader's own budget counts the rows.
     */
    public void testElementDropsSpendTheErrorRatio() throws Exception {
        byte[] data = write(MILLIS_ELEMENTS, factory -> {
            List<Group> groups = new ArrayList<>();
            for (long r = 0; r < 20; r++) {
                Group g = factory.newGroup().append("id", r).append("n", r);
                g.addGroup("items").addGroup("list").addGroup("element").append("at", YEAR_3000);
                groups.add(g);
            }
            return groups;
        });
        ErrorPolicy tenPercent = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 1_000, 0.1, false);
        Exception e = expectThrows(
            Exception.class,
            () -> read(
                data,
                context(List.of("id"), leaf("at", DataType.DATE_NANOS)).errorPolicy(tenPercent).informationalWarningSink(w -> {})
            )
        );
        assertThat(causeMessages(e), containsString("dropped rows"));
    }

    /**
     * A parent dropped for a value of one of its columns and a parent dropped for an element value are two errors of one
     * budget, not one error each of two.
     */
    public void testColumnAndElementDropsShareTheErrorCount() throws Exception {
        byte[] data = write(MILLIS_ELEMENTS, factory -> {
            List<Group> groups = new ArrayList<>();
            for (long r = 0; r < 6; r++) {
                Group g = factory.newGroup().append("id", r).append("n", r == 1 ? 1L << 40 : r);
                g.addGroup("items").addGroup("list").addGroup("element").append("at", r == 4 ? YEAR_3000 : 1_000L);
                groups.add(g);
            }
            return groups;
        });
        List<Attribute> readSchema = List.of(
            new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, "n", DataType.INTEGER)
        );
        UnaryOperator<ParquetFormatReader> declared = r -> (ParquetFormatReader) r.withDeclaredTypeColumns(Set.of("n"));
        Function<Long, FormatReadContext.Builder> context = maxErrors -> context(List.of("id", "n"), leaf("at", DataType.DATE_NANOS))
            .readSchema(readSchema)
            .errorPolicy(new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, maxErrors, 1.0, false))
            .informationalWarningSink(w -> {});
        Exception e = expectThrows(Exception.class, () -> read(data, context.apply(1L), declared));
        assertThat(causeMessages(e), containsString("dropped rows"));
        Expanded read = read(data, context.apply(2L), declared);
        assertThat(read.rows.stream().map(row -> row.get(0)).toList(), equalTo(List.of(0L, 2L, 3L, 5L)));
    }

    /** A list of timestamps in milliseconds as the element, beside a parent column {@code n}. */
    private static final MessageType MILLIS_ELEMENTS = MessageTypeParser.parseMessageType("""
        message t {
          required int64 id;
          optional int64 n;
          optional group items (LIST) {
            repeated group list {
              optional group element {
                optional int64 at (TIMESTAMP(MILLIS, true));
              }
            }
          }
        }
        """);

    /** 3000-01-01T00:00:00Z in milliseconds, which has no date_nanos value. */
    private static final long YEAR_3000 = 32_503_680_000_000L;

    /**
     * Under {@code skip_row} against a finite budget, the dropped parents are charged one error each, across batches, so a budget of one
     * error passes one dropped parent and fails at the second, whatever the number of their elements.
     */
    public void testElementCastFailuresAcrossBatchesSpendTheBudgetPerParent() throws Exception {
        MessageType millis = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    optional int64 at (TIMESTAMP(MILLIS, true));
                  }
                }
              }
            }
            """);
        long year3000 = 32_503_680_000_000L;
        Function<Set<Long>, byte[]> fixture = bad -> {
            try {
                return write(millis, factory -> {
                    List<Group> groups = new ArrayList<>();
                    for (long r = 0; r < 20; r++) {
                        Group g = factory.newGroup().append("id", r);
                        Group items = g.addGroup("items");
                        for (int e = 0; e < 5; e++) {
                            items.addGroup("list").addGroup("element").append("at", bad.contains(r) ? year3000 : 1_000L);
                        }
                        groups.add(g);
                    }
                    return groups;
                }, 7, 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        ErrorPolicy oneError = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 1, 1.0, false);
        Expanded read = read(
            fixture.apply(Set.of(9L)),
            context(List.of("id"), leaf("at", DataType.DATE_NANOS)).batchSize(10).errorPolicy(oneError).informationalWarningSink(w -> {})
        );
        assertThat(read.rows.size(), equalTo(19 * 5));
        assertThat("the batches cut through the file", read.pageRows.size(), greaterThan(2));

        Exception e = expectThrows(
            Exception.class,
            () -> read(
                fixture.apply(Set.of(3L, 15L)),
                context(List.of("id"), leaf("at", DataType.DATE_NANOS)).batchSize(10)
                    .errorPolicy(oneError)
                    .informationalWarningSink(w -> {})
            )
        );
        assertThat(causeMessages(e), containsString("dropped rows"));
    }

    /** The projection holds the requested leaves of the element and none of its other leaves. */
    public void testOnlyRequestedLeavesProjected() {
        List<Attribute> id = List.of(new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG));
        MessageType projected = ParquetFormatReader.projectedSchema(
            SCHEMA,
            id,
            spec(SCHEMA, leaf("sku", DataType.KEYWORD), leaf("tags", DataType.KEYWORD))
        );
        assertThat(
            projected.getColumns().stream().map(c -> String.join(".", c.getPath())).toList(),
            equalTo(List.of("id", "items.list.element.sku", "items.list.element.tags.list.element"))
        );
        MessageType structural = ParquetFormatReader.projectedSchema(SCHEMA, id, spec(SCHEMA, leaf("color", DataType.KEYWORD)));
        assertThat(
            structural.getColumns().stream().map(c -> String.join(".", c.getPath())).toList(),
            equalTo(List.of("id", "items.list.element.pid"))
        );
    }

    /**
     * With no parent column projected, the read decodes no parent column and the page holds the element columns only;
     * a pushed filter's own column, read for the filter, is not part of the output either.
     */
    public void testEmptyParentProjectionReadsNoParentColumn() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                Group g = factory.newGroup().append("id", (long) r).append("label", "l" + r);
                g.addGroup("items").addGroup("list").addGroup("element").append("pid", (long) r);
                groups.add(g);
            }
            return groups;
        });
        Expanded read = read(data, context(List.of(), leaf("pid", DataType.LONG)));
        assertThat(read.rows, equalTo(List.of(List.of(0L), List.of(1L), List.of(2L), List.of(3L))));
        assertThat(read.blockCounts, equalTo(List.of(1)));
        assertThat(
            ParquetFormatReader.projectedSchema(SCHEMA, List.of(), spec(SCHEMA, leaf("pid", DataType.LONG)))
                .getColumns()
                .stream()
                .map(c -> String.join(".", c.getPath()))
                .toList(),
            equalTo(List.of("items.list.element.pid"))
        );

        ReferenceAttribute id = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, id, new Literal(Source.EMPTY, 2L, DataType.LONG), null);
        Expanded filtered = read(
            data,
            context(List.of(), leaf("pid", DataType.LONG)),
            r -> r.withPushedFilter(new ParquetPushedExpressions(List.of(filter)))
        );
        for (int blocks : filtered.blockCounts) {
            assertThat(blocks, equalTo(1));
        }
        if (optimized) {
            assertThat(filtered.rows, equalTo(List.of(List.of(0L), List.of(1L))));
        }
    }

    /**
     * A batch ends at the row where the rows it expands to reach the batch size, so no page holds more than a batch plus
     * its last parent's elements, and the rows of the whole read are every element plus one null row per parent without
     * any. A single parent larger than the batch is held whole.
     */
    public void testBatchCutAtExpandedRowBudget() throws Exception {
        int rows = 60;
        int batchSize = 500;
        int huge = batchSize + 10;
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r <= rows; r++) {
                Group g = factory.newGroup().append("id", (long) r);
                int count = r == rows ? huge : r % 4 == 0 ? 0 : 70;
                if (count > 0) {
                    Group items = g.addGroup("items");
                    for (int e = 0; e < count; e++) {
                        items.addGroup("list").addGroup("element").append("pid", (long) r);
                    }
                }
                groups.add(g);
            }
            return groups;
        });
        Expanded read = read(data, context(List.of("id"), leaf("pid", DataType.LONG)).batchSize(batchSize));
        // Rows 0, 4, ... 56 have no elements; row 60, the last, is the huge one
        long childless = rows / 4;
        long elements = (rows - childless) * 70 + huge;
        assertThat((long) read.rows.size(), equalTo(elements + childless));
        assertThat("the batches must have been cut", read.pageRows.size(), greaterThan(2));
        int at = 0;
        for (int pageRows : read.pageRows) {
            List<List<Object>> page = read.rows.subList(at, at + pageRows);
            Object lastParent = page.getLast().get(0);
            long lastParentRows = page.stream().filter(row -> row.get(0).equals(lastParent)).count();
            // The batch stops at the row that reaches the budget: without its last parent it stayed under it
            assertThat(pageRows - lastParentRows, lessThan((long) batchSize));
            at += pageRows;
        }
        List<List<Object>> lastPage = read.rows.subList(read.rows.size() - read.pageRows.getLast(), read.rows.size());
        long hugeRows = lastPage.stream().filter(row -> row.get(0).equals((long) rows)).count();
        assertThat("the parent above the budget is held whole, in one page", hugeRows, equalTo((long) huge));
        for (List<Object> row : read.rows) {
            if (row.get(1) != null) {
                assertThat(row.get(1), equalTo(row.get(0)));
            }
        }
    }

    /**
     * A pushed filter that drops parents in the middle of a batch drops them before they are expanded: their elements
     * go with them and every surviving element row carries its own parent's values. The optimized iterator applies the
     * filter itself, through late materialization; the baseline one leaves the rows to a filter above it.
     */
    public void testPushedFilterDropsParentsBeforeExpansion() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 10; r++) {
                Group g = factory.newGroup().append("id", (long) r).append("label", "l" + r);
                Group items = g.addGroup("items");
                for (int e = 0; e <= r % 3; e++) {
                    items.addGroup("list").addGroup("element").append("pid", (long) r).append("sku", "s" + r + "_" + e);
                }
                groups.add(g);
            }
            return groups;
        });
        ReferenceAttribute id = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new NotEquals(Source.EMPTY, id, new Literal(Source.EMPTY, 4L, DataType.LONG), null);
        Expanded read = read(
            data,
            context(List.of("id", "label"), leaf("pid", DataType.LONG), leaf("sku", DataType.KEYWORD)),
            r -> r.withPushedFilter(new ParquetPushedExpressions(List.of(filter)))
        );
        List<Long> parents = read.rows.stream().map(row -> (Long) row.get(0)).distinct().toList();
        List<Long> survivors = List.of(0L, 1L, 2L, 3L, 5L, 6L, 7L, 8L, 9L);
        if (optimized) {
            assertThat(parents, equalTo(survivors));
        } else {
            // The baseline iterator may keep the filtered parent for the filter above, but never loses a survivor
            assertTrue(parents.containsAll(survivors));
        }
        for (List<Object> row : read.rows) {
            assertThat(row.get(2), equalTo(row.get(0)));
            assertThat(row.get(1), equalTo(ref("l" + row.get(0))));
        }
        long expected = parents.stream().mapToLong(p -> p % 3 + 1).sum();
        assertThat((long) read.rows.size(), equalTo(expected));
    }

    /**
     * A {@code skip_row} drop of parents in the middle of a batch, from a value of a parent column that cannot be read
     * as its declared type, drops their elements with them on both iterators; the rows left are exactly the rows of the
     * surviving parents.
     */
    public void testSkipRowDropsParentsInTheMiddleOfABatch() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 8; r++) {
                Group g = factory.newGroup().append("id", (long) r).append("n", r == 2 || r == 5 ? 1L << 40 : (long) r);
                Group items = g.addGroup("items");
                for (int e = 0; e < 2; e++) {
                    items.addGroup("list").addGroup("element").append("pid", (long) r);
                }
                groups.add(g);
            }
            return groups;
        });
        List<Attribute> readSchema = List.of(
            new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, "n", DataType.INTEGER)
        );
        List<String> warnings = new ArrayList<>();
        Expanded read = read(
            data,
            context(List.of("id", "n"), leaf("pid", DataType.LONG)).readSchema(readSchema)
                .errorPolicy(new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, Long.MAX_VALUE, 0.0, false))
                .informationalWarningSink(warnings::add),
            r -> (ParquetFormatReader) r.withDeclaredTypeColumns(Set.of("n"))
        );
        assertThat(warnings, hasItem(containsString("cannot be read as their declared type; skipping their rows")));
        List<List<Object>> expected = new ArrayList<>();
        for (long r : List.of(0L, 1L, 3L, 4L, 6L, 7L)) {
            expected.add(Arrays.asList(r, (int) r, r));
            expected.add(Arrays.asList(r, (int) r, r));
        }
        assertThat(read.rows, equalTo(expected));
        assertThat("the drop happened inside one batch", read.pageRows, equalTo(List.of(12)));
    }

    /**
     * The row limit counts parents, before they are expanded, so a limit of five keeps every row of five parents. The
     * batch budget cuts the read into batches of two parents, so the limit is carried across batches and a count of
     * expanded rows would stop it after the first.
     */
    public void testRowLimitCountsParents() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 10; r++) {
                Group g = factory.newGroup().append("id", (long) r);
                if (r != 1) {
                    Group items = g.addGroup("items");
                    for (int e = 0; e < 3; e++) {
                        items.addGroup("list").addGroup("element").append("pid", (long) r);
                    }
                }
                groups.add(g);
            }
            return groups;
        });
        Expanded read = read(data, context(List.of("id"), leaf("pid", DataType.LONG)).batchSize(4).rowLimit(5));
        assertThat(read.rows.stream().map(row -> row.get(0)).distinct().toList(), equalTo(List.of(0L, 1L, 2L, 3L, 4L)));
        assertThat(read.rows.size(), equalTo(3 + 1 + 3 + 3 + 3));
        assertThat("the limit spans several batches", read.pageRows.size(), greaterThan(1));
    }

    /**
     * A row position addresses a file row, so a read that expands refuses to carry one. Both read contexts refuse it
     * when they are built, which is the reader's only way in, so the reader keeps no check of its own.
     */
    public void testRowPositionsRefused() {
        ExpandRead expand = new ExpandRead("items", List.of(leaf("pid", DataType.LONG)));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FormatReadContext.builder()
                .projectedColumns(List.of("id", ColumnExtractor.ROW_POSITION_COLUMN))
                .batchSize(10)
                .expandRead(expand)
                .build()
        );
        assertThat(e.getMessage(), containsString("cannot carry [" + ColumnExtractor.ROW_POSITION_COLUMN + "]"));
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new RangeReadContext(
                List.of("id", ColumnExtractor.ROW_POSITION_COLUMN),
                10,
                0,
                1,
                List.of(),
                ErrorPolicy.STRICT,
                null,
                FormatReader.NO_LIMIT,
                null,
                null,
                expand
            )
        );
        assertThat(e.getMessage(), containsString("cannot carry [" + ColumnExtractor.ROW_POSITION_COLUMN + "]"));
    }

    /**
     * Every block is charged to the read's breaker and released by the time the read is closed, whether the read
     * finished or a breaker trip at any point of it failed it. The limits span reads that cannot start to reads that
     * finish.
     */
    public void testBreakerReleasedOnSuccessAndFailure() throws Exception {
        byte[] data = breakerFixture();
        int tripped = 0;
        int finished = 0;
        for (long limit = 1024; limit <= 8 * 1024 * 1024; limit *= 2) {
            LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(limit));
            blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
            try {
                Expanded read = read(data, breakerContext());
                assertThat(read.rows.size(), equalTo(48 * 40 + 12));
                finished++;
            } catch (CircuitBreakingException e) {
                tripped++;
            }
            assertThat("breaker at limit " + limit, breaker.getUsed(), equalTo(0L));
        }
        assertThat("some limit must trip the breaker", tripped, greaterThan(0));
        assertThat("some limit must let the read finish", finished, greaterThan(0));
    }

    /**
     * Where a trip lands. A breaker that trips on the n-th charge of one label fails the read exactly there: in the
     * join's position arrays ({@code expand_read}) or in a block the decode or the parent columns build. Either way the
     * read fails with the trip and leaves the breaker at zero.
     */
    public void testBreakerTripInTheJoinOrInABlockReleasesEverything() throws Exception {
        byte[] data = breakerFixture();
        for (String label : List.of("expand_read", "<esql_block_factory>")) {
            for (int nth : new int[] { 1, 3, 10 }) {
                TrippingBreaker breaker = new TrippingBreaker(label, nth);
                blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
                expectThrows(CircuitBreakingException.class, () -> read(data, breakerContext()));
                assertThat(label + " #" + nth, breaker.tripped, equalTo(true));
                assertThat(label + " #" + nth, breaker.getUsed(), equalTo(0L));
            }
        }
    }

    /** Trips on the {@code nth} charge labelled {@code label}, and charges everything else without a limit. */
    /**
     * A column the row reads and the expanded collection cannot share a column reader, so a read projecting the
     * collection, or a field of it, beside its expansion is refused, whichever iterator reads it.
     */
    public void testColumnOfTheExpandedCollectionIsRefused() throws Exception {
        byte[] data = write(SCHEMA, factory -> List.of(factory.newGroup().append("id", 1L)));
        for (String column : List.of("items", "items.pid")) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> read(data, context(List.of("id", column), leaf("pid", DataType.LONG)))
            );
            assertThat(e.getMessage(), equalTo("column [" + column + "] cannot be read both as a column and as part of expanded [items]"));
        }
    }

    /**
     * Under {@code skip_row}, the arrays that track which elements failed their cast, and which parents they drop, are
     * charged to the read's breaker as the join's own position arrays are, and released with the batch.
     */
    public void testFailedElementsAreChargedToTheBreaker() throws Exception {
        MessageType millis = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list { optional group element { optional int64 at (TIMESTAMP(MILLIS, true)); } }
              }
            }
            """);
        byte[] data = write(millis, factory -> {
            List<Group> groups = new ArrayList<>();
            for (long r = 0; r < 4; r++) {
                Group g = factory.newGroup().append("id", r);
                Group items = g.addGroup("items");
                for (int e = 0; e < 3; e++) {
                    items.addGroup("list").addGroup("element").append("at", r == 1 && e == 1 ? 32_503_680_000_000L : 1_000L);
                }
                groups.add(g);
            }
            return groups;
        });
        List<Long> charges = new ArrayList<>();
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofGb(1)) {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                if (ExpandedRows.BREAKER_LABEL.equals(label)) {
                    charges.add(bytes);
                }
                super.addEstimateBytesAndMaybeBreak(bytes, label);
            }
        };
        blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        ErrorPolicy skipRow = new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, Long.MAX_VALUE, 1.0, false);
        Expanded read = read(
            data,
            context(List.of("id"), leaf("at", DataType.DATE_NANOS)).errorPolicy(skipRow).informationalWarningSink(w -> {})
        );
        assertThat(read.rows.size(), equalTo(9));
        // One flag per element of the batch, then per batch row a flag and two kept positions
        assertThat(charges, hasItem(12L));
        assertThat(charges, hasItem(4L + 2L * 4 * Integer.BYTES));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static final class TrippingBreaker extends LimitedBreaker {
        private final String label;
        private int remaining;
        private boolean tripped;

        TrippingBreaker(String label, int nth) {
            super("test", ByteSizeValue.ofGb(1));
            this.label = label;
            this.remaining = nth;
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String chargeLabel) {
            if (tripped == false && label.equals(chargeLabel) && bytes > 0 && --remaining == 0) {
                tripped = true;
                throw new CircuitBreakingException("tripped on [" + chargeLabel + "]", bytes, 0, CircuitBreaker.Durability.TRANSIENT);
            }
            super.addEstimateBytesAndMaybeBreak(bytes, chargeLabel);
        }
    }

    private static byte[] breakerFixture() throws IOException {
        return write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 60; r++) {
                Group g = factory.newGroup().append("id", (long) r).append("label", "label" + r);
                if (r % 5 != 0) {
                    Group items = g.addGroup("items");
                    for (int e = 0; e < 40; e++) {
                        items.addGroup("list").addGroup("element").append("pid", (long) r).append("sku", "s" + e);
                    }
                }
                groups.add(g);
            }
            return groups;
        });
    }

    private static FormatReadContext.Builder breakerContext() {
        return context(List.of("id", "label"), leaf("pid", DataType.LONG), leaf("sku", DataType.KEYWORD), leaf("qty", DataType.LONG))
            .batchSize(64);
    }

    /**
     * The legacy two-level list and the three-level one each read with one requested leaf and with several. With one
     * leaf the pruned element is a group of one field, which reads as a list of that primitive if the shape is derived
     * from the projection rather than the file.
     */
    public void testTwoAndThreeLevelListsWithOneAndSeveralLeaves() throws Exception {
        byte[] twoLevel = write(TWO_LEVEL, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("element").append("pid", 0L).append("sku", "a");
            items.addGroup("element").append("pid", 0L).append("sku", "b");
            Group r1 = factory.newGroup().append("id", 1L);
            r1.addGroup("items").addGroup("element").append("pid", 1L).append("sku", "c");
            return List.of(r0, r1, factory.newGroup().append("id", 2L));
        });
        byte[] threeLevel = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("pid", 0L).append("sku", "a");
            items.addGroup("list").addGroup("element").append("pid", 0L).append("sku", "b");
            Group r1 = factory.newGroup().append("id", 1L);
            r1.addGroup("items").addGroup("list").addGroup("element").append("pid", 1L).append("sku", "c");
            return List.of(r0, r1, factory.newGroup().append("id", 2L));
        });
        for (byte[] data : List.of(twoLevel, threeLevel)) {
            Expanded one = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD)));
            assertThat(
                one.rows,
                equalTo(
                    List.of(Arrays.asList(0L, ref("a")), Arrays.asList(0L, ref("b")), Arrays.asList(1L, ref("c")), Arrays.asList(2L, null))
                )
            );
            Expanded several = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD), leaf("pid", DataType.LONG)));
            assertThat(
                several.rows,
                equalTo(
                    List.of(
                        Arrays.asList(0L, ref("a"), 0L),
                        Arrays.asList(0L, ref("b"), 0L),
                        Arrays.asList(1L, ref("c"), 1L),
                        Arrays.asList(2L, null, null)
                    )
                )
            );
        }
    }

    /** A required list of required elements, with a structure inside the element named by a dotted leaf. */
    public void testRequiredElementAndStruct() throws Exception {
        byte[] data = write(REQUIRED, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items0 = r0.addGroup("items");
            items0.addGroup("list").addGroup("element").append("pid", 0L).addGroup("dim").append("sku", "a");
            items0.addGroup("list").addGroup("element").append("pid", 0L).addGroup("dim").append("sku", "b");
            Group r1 = factory.newGroup().append("id", 1L);
            r1.addGroup("items");
            return List.of(r0, r1);
        });
        Expanded read = read(data, context(List.of("id"), leaf("dim.sku", DataType.KEYWORD), leaf("pid", DataType.LONG)));
        assertThat(
            read.rows,
            equalTo(List.of(Arrays.asList(0L, ref("a"), 0L), Arrays.asList(0L, ref("b"), 0L), Arrays.asList(1L, null, null)))
        );
    }

    /**
     * Elements cross row-group boundaries correctly, and a column chunk of several dictionary-encoded pages decodes as
     * one; the file is checked to have both, so the test cannot pass on a single page.
     */
    public void testMultipleRowGroupsAndPagesWithDictionary() throws Exception {
        int rows = 400;
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < rows; r++) {
                Group g = factory.newGroup().append("id", (long) r);
                Group items = g.addGroup("items");
                for (int e = 0; e < r % 4; e++) {
                    items.addGroup("list").addGroup("element").append("pid", (long) r).append("sku", "sku" + (e % 3));
                }
                groups.add(g);
            }
            return groups;
        }, 100, 20);
        ParquetReadOptions options = PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build();
        try (ParquetFileReader file = ParquetFileReader.open(storageAdapter(data), options)) {
            List<BlockMetaData> rowGroups = file.getRowGroups();
            assertThat(rowGroups.size(), greaterThan(1));
            ColumnChunkMetaData sku = rowGroups.getFirst()
                .getColumns()
                .stream()
                .filter(c -> c.getPath().toDotString().equals("items.list.element.sku"))
                .findFirst()
                .orElseThrow();
            assertTrue("dictionary-encoded", sku.hasDictionaryPage());
            assertThat(file.readOffsetIndex(sku).getPageCount(), greaterThan(1));
        }
        Expanded read = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD), leaf("pid", DataType.LONG)).batchSize(37));
        List<List<Object>> expected = new ArrayList<>();
        for (long r = 0; r < rows; r++) {
            if (r % 4 == 0) {
                expected.add(Arrays.asList(r, null, null));
            }
            for (int e = 0; e < r % 4; e++) {
                expected.add(Arrays.asList(r, ref("sku" + (e % 3)), r));
            }
        }
        assertThat(read.rows, equalTo(expected));
    }

    /**
     * An element's field reads as the same field does as a column, for every physical and logical type a column can
     * have and for a list, a list of lists, a list of structures and a variant: each value is written once as a column of
     * the file and once in the element, and the two reads must agree. A field the reader cannot read, a map among them,
     * is null, as the same column is.
     */
    public void testElementFieldsReadAsTheirColumns() throws Exception {
        List<Type> columns = new ArrayList<>(ParquetFormatReaderTests.everyColumnType("c_"));
        columns.addAll(ParquetFormatReaderTests.everyGroupShape("c_"));
        List<Type> fields = new ArrayList<>(ParquetFormatReaderTests.everyColumnType(""));
        fields.addAll(ParquetFormatReaderTests.everyGroupShape(""));
        fields.add(MessageTypeParser.parseMessageType("""
            message m { optional group m (MAP) { repeated group key_value { required binary key (UTF8); optional int64 value; } } }
            """).getType("m"));
        MessageType schema = Types.buildMessage()
            .addFields(columns.toArray(Type[]::new))
            .addField(
                Types.optionalGroup()
                    .as(LogicalTypeAnnotation.listType())
                    .addField(
                        Types.repeatedGroup()
                            .addField(Types.optionalGroup().addFields(fields.toArray(Type[]::new)).named("element"))
                            .named("list")
                    )
                    .named("items")
            )
            .named("t");
        byte[] data = write(schema, factory -> {
            Group row = factory.newGroup();
            appendEveryColumnType(row, "c_");
            appendEveryGroupShape(row, "c_");
            Group element = row.addGroup("items").addGroup("list").addGroup("element");
            appendEveryColumnType(element, "");
            appendEveryGroupShape(element, "");
            element.addGroup("m").addGroup("key_value").append("key", "k").append("value", 1L);
            return List.of(row);
        });
        List<ExpandRead.Leaf> leaves = ParquetCollections.describe(schema)
            .stream()
            .filter(collection -> collection.path().equals("items"))
            .findFirst()
            .orElseThrow()
            .leaves()
            .stream()
            .map(leaf -> leaf(leaf.name(), leaf.type()))
            .toList();
        List<String> names = leaves.stream().map(ExpandRead.Leaf::name).toList();
        List<String> projected = names.stream().filter(name -> name.equals("m") == false).map(name -> "c_" + name).toList();
        Expanded read = read(data, context(projected, leaves.toArray(ExpandRead.Leaf[]::new)));
        assertThat(read.rows.size(), equalTo(1));
        List<Object> row = read.rows.getFirst();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Object element = row.get(projected.size() + i);
            if (name.equals("m")) {
                assertThat(name, element, nullValue());
            } else {
                assertThat(name, element, equalTo(row.get(projected.indexOf("c_" + name))));
            }
        }
        // The values decoded, so the comparison above is not one of nulls
        assertThat(row.get(projected.indexOf("c_dec_bin")), equalTo(-12345678901234567.8901));
        assertThat(row.get(projected.indexOf("c_uuid")), equalTo(new BytesRef("00010203-0405-0607-0809-0a0b0c0d0e0f")));
        assertThat(row.get(projected.indexOf("c_ints")), equalTo(List.of(1, 2)));
    }

    /** Writes one value of each field of {@link ParquetFormatReaderTests#everyGroupShape} named with {@code prefix}. */
    private static void appendEveryGroupShape(Group group, String prefix) {
        Group ints = group.addGroup(prefix + "ints");
        ints.addGroup("list").append("element", 1);
        ints.addGroup("list").append("element", 2);
        group.addGroup(prefix + "lists").addGroup("list").addGroup("element").addGroup("list").append("element", 3);
        group.addGroup(prefix + "structs").addGroup("list").addGroup("element").append("a", 4L);
        group.addGroup(prefix + "v")
            .append("metadata", Binary.fromConstantByteArray(new byte[] { 1, 0, 0 }))
            .append("value", Binary.fromConstantByteArray(new byte[] { 0 }));
    }

    /**
     * A field one file stores as a map, which the reader cannot read, and another as text is merged to keyword. The first
     * file cannot supply it, so its elements read it as null with the warning a column of a type that does not widen
     * raises, rather than silently.
     */
    public void testLeafUnsupportedInTheFileButNotAsMergedReadsAsNullWithAWarning() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    optional int64 pid;
                    optional group payload (MAP) { repeated group key_value { required binary key (UTF8); optional int64 value; } }
                  }
                }
              }
            }
            """);
        byte[] data = write(schema, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group element = r0.addGroup("items").addGroup("list").addGroup("element").append("pid", 7L);
            element.addGroup("payload").addGroup("key_value").append("key", "k").append("value", 1L);
            return List.of(r0);
        });
        List<String> warnings = new ArrayList<>();
        Expanded read = read(
            data,
            context(List.of("id"), leaf("pid", DataType.LONG), leaf("payload", DataType.KEYWORD)).informationalWarningSink(warnings::add)
        );
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, 7L, null))));
        assertThat(warnings, hasItem(containsString("column [items.payload]: [unsupported] in the file, [keyword] in the query")));

        // Unsupported as merged too: no file can supply it, so it is null with no warning, as an unsupported column is
        warnings.clear();
        read = read(
            data,
            context(List.of("id"), leaf("pid", DataType.LONG), leaf("payload", DataType.UNSUPPORTED)).informationalWarningSink(
                warnings::add
            )
        );
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, 7L, null))));
        assertThat(warnings, equalTo(List.of()));
    }

    /**
     * A request whose only leaves are ones the reader cannot read, a map and a variant, still expands: the elements come
     * from the levels of another leaf, and each reads as null.
     */
    public void testOnlyUnsupportedLeavesRequested() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    optional int64 pid;
                    optional group m (MAP) { repeated group key_value { required binary key (UTF8); optional int64 value; } }
                    optional group v (VARIANT(1)) { required binary metadata; required binary value; }
                  }
                }
              }
            }
            """);
        byte[] data = write(schema, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("pid", 1L);
            items.addGroup("list").addGroup("element").append("pid", 2L);
            return List.of(r0, factory.newGroup().append("id", 1L));
        });
        assertThat(
            ParquetCollections.describe(schema).getFirst().leaves(),
            equalTo(
                List.of(
                    new ParquetCollections.Leaf("m", DataType.UNSUPPORTED),
                    new ParquetCollections.Leaf("pid", DataType.LONG),
                    new ParquetCollections.Leaf("v", DataType.UNSUPPORTED)
                )
            )
        );
        Expanded read = read(data, context(List.of("id"), leaf("m", DataType.UNSUPPORTED), leaf("v", DataType.UNSUPPORTED)));
        assertThat(
            read.rows,
            equalTo(List.of(Arrays.asList(0L, null, null), Arrays.asList(0L, null, null), Arrays.asList(1L, null, null)))
        );
    }

    /** Writes one value of each field of {@link ParquetFormatReaderTests#everyColumnType} named with {@code prefix}. */
    private static void appendEveryColumnType(Group group, String prefix) {
        byte[] uuid = new byte[16];
        for (int i = 0; i < uuid.length; i++) {
            uuid[i] = (byte) i;
        }
        byte[] int96 = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).putLong(3_600_000_000_000L).putInt(2_460_000).array();
        byte[] decimalFixed = new byte[9];
        byte[] unscaled = BigInteger.valueOf(12_345_678_901L).toByteArray();
        System.arraycopy(unscaled, 0, decimalFixed, decimalFixed.length - unscaled.length, unscaled.length);
        group.append(prefix + "bool", true)
            .append(prefix + "i32", -7)
            .append(prefix + "u8", 200)
            .append(prefix + "u32", -1)
            .append(prefix + "date", 19_000)
            .append(prefix + "time_ms", 3_723_004)
            .append(prefix + "dec32", -12_345)
            .append(prefix + "i64", 1L << 40)
            .append(prefix + "u64", -1L)
            .append(prefix + "ts_ms", 1_700_000_000_123L)
            .append(prefix + "ts_us", 1_700_000_000_123_456L)
            .append(prefix + "ts_ns", 1_700_000_000_123_456_789L)
            .append(prefix + "time_us", 3_723_004_005L)
            .append(prefix + "time_ns", 3_723_004_005_006L)
            .append(prefix + "dec64", -987_654_321_012L)
            .append(prefix + "i96", Binary.fromConstantByteArray(int96))
            .append(prefix + "f32", 1.25f)
            .append(prefix + "f64", -2.5d)
            .append(prefix + "str", "text")
            .append(prefix + "raw", Binary.fromConstantByteArray(new byte[] { (byte) 0xFF, 'a' }))
            .append(prefix + "enum", "RED")
            .append(prefix + "json", "{\"a\":1}")
            .append(prefix + "bson", Binary.fromConstantByteArray(new byte[] { 5, 0, 0, 0, 0 }))
            .append(prefix + "dec_bin", Binary.fromConstantByteArray(new BigInteger("-123456789012345678901").toByteArray()))
            .append(prefix + "uuid", Binary.fromConstantByteArray(uuid))
            .append(prefix + "f16", Binary.fromConstantByteArray(new byte[] { 0x00, 0x3E }))
            .append(prefix + "interval", Binary.fromConstantByteArray(new byte[12]))
            .append(prefix + "fixed", Binary.fromConstantByteArray("abcdefgh".getBytes(StandardCharsets.UTF_8)))
            .append(prefix + "dec_fixed", Binary.fromConstantByteArray(decimalFixed));
    }

    /**
     * A keyword element value that is not valid UTF-8 is repaired to U+FFFD, as a keyword column's is, in a scalar leaf
     * and in a list inside the element: keyword operations index tables by the lead byte.
     */
    public void testMalformedKeywordElementIsRepaired() throws Exception {
        Binary malformed = Binary.fromConstantByteArray(new byte[] { (byte) 0xFF });
        byte[] replacement = { (byte) 0xEF, (byte) 0xBF, (byte) 0xBD };
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group element = r0.addGroup("items").addGroup("list").addGroup("element").append("sku", malformed);
            element.addGroup("tags").addGroup("list").append("element", malformed);
            return List.of(r0);
        });
        Expanded read = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD), leaf("tags", DataType.KEYWORD)));
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, new BytesRef(replacement), new BytesRef(replacement)))));
    }

    /**
     * Conversions from a leaf's physical value to its type: boolean, date, unsigned int, micros, float and double. Every
     * other type is read against its column by {@link #testElementFieldsReadAsTheirColumns}.
     */
    public void testValueKinds() throws Exception {
        byte[] data = write(KINDS, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            r0.addGroup("items")
                .addGroup("list")
                .addGroup("element")
                .append("b", true)
                .append("d", 19000)
                .append("u", -1)
                .append("ts", 1_000_000L)
                .append("f", 1.5f)
                .append("x", 2.25);
            return List.of(r0);
        });
        Expanded read = read(
            data,
            context(
                List.of("id"),
                leaf("b", DataType.BOOLEAN),
                leaf("d", DataType.DATETIME),
                leaf("u", DataType.LONG),
                leaf("ts", DataType.DATE_NANOS),
                leaf("f", DataType.DOUBLE),
                leaf("x", DataType.DOUBLE)
            )
        );
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, true, 19000L * 86_400_000L, 4294967295L, 1_000_000_000L, 1.5d, 2.25d))));
    }

    /**
     * A {@code TIMESTAMP(MICROS)} element value whose nanoseconds leave the {@code date_nanos} range reads as null
     * with the warning a column of that type gives, rather than wrapping to another instant. In a list inside the element
     * only the out-of-range value is left out; the element's in-range values stay.
     */
    public void testMicrosTimestampOutOfRangeIsNullWithAWarning() throws Exception {
        MessageType micros = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    optional int64 at (TIMESTAMP(MICROS, true));
                    optional group ats (LIST) { repeated group list { optional int64 element (TIMESTAMP(MICROS, true)); } }
                  }
                }
              }
            }
            """);
        // 9999-12-31T23:59:59Z, the usual open-ended sentinel, has no nanosecond instant
        long sentinel = 253_402_300_799_000_000L;
        byte[] data = write(micros, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            Group e0 = items.addGroup("list").addGroup("element").append("at", sentinel);
            Group ats = e0.addGroup("ats");
            ats.addGroup("list").append("element", 5L);
            ats.addGroup("list").append("element", sentinel);
            items.addGroup("list").addGroup("element").append("at", 7L);
            return List.of(r0);
        });
        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        Expanded read = read(
            data,
            context(List.of("id"), leaf("at", DataType.DATE_NANOS), leaf("ats", DataType.DATE_NANOS)).informationalWarningSink(
                warnings::add
            )
        );
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, null, 5_000L), Arrays.asList(0L, 7_000L, null))));
        assertThat(
            warnings,
            hasItem(containsString("timestamps outside the [date_nanos] range (1677-09-21 to 2262-04-11); returning null"))
        );
        assertThat(warnings, hasItem(containsString("column [items.at]")));
        assertThat(warnings, hasItem(containsString("column [items.ats]")));
    }

    /**
     * The audit of the other value conversions against their column twins, at the edges of their ranges: a date's days
     * as milliseconds and an unsigned 32-bit value as a long are exact for every value the physical type holds, and a
     * float widens to the same double a float column reads as.
     */
    public void testValueKindsAtTheirRangeEdges() throws Exception {
        byte[] data = write(KINDS, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("d", Integer.MAX_VALUE).append("u", Integer.MIN_VALUE).append("f", 0.1f);
            items.addGroup("list").addGroup("element").append("d", Integer.MIN_VALUE).append("u", 0).append("f", Float.MAX_VALUE);
            return List.of(r0);
        });
        Expanded read = read(
            data,
            context(List.of("id"), leaf("d", DataType.DATETIME), leaf("u", DataType.LONG), leaf("f", DataType.DOUBLE))
        );
        assertThat(
            read.rows,
            equalTo(
                List.of(
                    Arrays.asList(0L, (long) Integer.MAX_VALUE * 86_400_000L, 2_147_483_648L, (double) 0.1f),
                    Arrays.asList(0L, (long) Integer.MIN_VALUE * 86_400_000L, 0L, (double) Float.MAX_VALUE)
                )
            )
        );
    }

    /**
     * A repeated primitive is a multivalued leaf, and a legacy repeated group, even of one field, is a collection of its own,
     * {@code items.codes}, which a read of {@code items} does not touch.
     */
    public void testLegacyRepeatedGroupInsideAnElementIsAnInnerCollection() throws Exception {
        MessageType legacy = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    repeated binary tags (UTF8);
                    repeated group codes { optional int64 code; }
                  }
                }
              }
            }
            """);
        assertThat(
            ParquetCollections.describe(legacy),
            equalTo(
                List.of(
                    new ParquetCollections.Collection("items.codes", List.of(new ParquetCollections.Leaf("code", DataType.LONG)), null),
                    new ParquetCollections.Collection("items", List.of(new ParquetCollections.Leaf("tags", DataType.KEYWORD)), null)
                )
            )
        );
        byte[] data = write(legacy, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            Group e0 = items.addGroup("list").addGroup("element").append("tags", "a").append("tags", "b");
            e0.addGroup("codes").append("code", 1L);
            e0.addGroup("codes").append("code", 2L);
            items.addGroup("list").addGroup("element").append("tags", "c");
            return List.of(r0);
        });
        Expanded read = read(data, context(List.of("id"), leaf("tags", DataType.KEYWORD)));
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, List.of(ref("a"), ref("b"))), Arrays.asList(0L, ref("c")))));
    }

    /**
     * An element whose only content is a collection of its own, with no field requested, still expands to one row per
     * element: its levels are read from the inner collection's column, whose repetitions inside an element are not elements.
     */
    public void testElementHoldingOnlyAnInnerCollection() throws Exception {
        MessageType schema = MessageTypeParser.parseMessageType("""
            message t {
              required int64 id;
              optional group items (LIST) {
                repeated group list {
                  optional group element {
                    optional group discounts (LIST) {
                      repeated group list { optional group element { optional binary code (UTF8); optional int64 pct; } }
                    }
                  }
                }
              }
            }
            """);
        byte[] data = write(schema, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            Group discounts = items.addGroup("list").addGroup("element").addGroup("discounts");
            discounts.addGroup("list").addGroup("element").append("code", "x").append("pct", 5L);
            discounts.addGroup("list").addGroup("element").append("code", "y").append("pct", 10L);
            items.addGroup("list").addGroup("element");
            items.addGroup("list").addGroup("element").addGroup("discounts").addGroup("list").addGroup("element").append("code", "z");
            Group r1 = factory.newGroup().append("id", 1L);
            return List.of(r0, r1);
        });
        Expanded read = read(data, context(List.of("id")));
        assertThat(read.rows.stream().map(row -> row.get(0)).toList(), equalTo(List.of(0L, 0L, 0L, 1L)));
    }

    /**
     * A failure inside the expansion is reported with where it happened, the collection, row group and batch. The
     * failure is injected with a test double, a breaker that refuses the first block built once three pages have been
     * read; the elements are decoded first, so that block is the expansion's. Two row groups of two parents, one parent a
     * batch, put the fourth batch at the second row group's second batch, so swapped or shifted numbers show. A corrupt
     * file would fail in the page reads before the expansion, and is not one parquet-mr will write.
     */
    public void testFailureInTheExpansionNamesWhereItHappened() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> rows = new ArrayList<>();
            for (long id = 0; id < 4; id++) {
                Group row = factory.newGroup().append("id", id);
                row.addGroup("items").addGroup("list").addGroup("element").append("pid", id);
                rows.add(row);
            }
            return rows;
        }, 2, 0);
        AtomicBoolean armed = new AtomicBoolean();
        blockFactory = new BlockFactory(new LimitedBreaker("test", ByteSizeValue.ofGb(1)) {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                if (bytes > 0 && label.equals("<esql_block_factory>") && armed.compareAndSet(true, false)) {
                    throw new IllegalArgumentException("injected");
                }
                super.addEstimateBytesAndMaybeBreak(bytes, label);
            }
        }, BigArrays.NON_RECYCLING_INSTANCE);
        ParquetFormatReader reader = reader();
        reader.clearFooterCachesForTests();
        FormatReadContext context = context(List.of("id"), leaf("pid", DataType.LONG)).batchSize(1).build();
        Exception e = expectThrows(Exception.class, () -> {
            try (CloseableIterator<Page> pages = reader.read(storage(data, false, new AtomicLong()), context)) {
                for (int page = 0; page < 3; page++) {
                    assertTrue(pages.hasNext());
                    pages.next().releaseBlocks();
                }
                armed.set(true);
                while (pages.hasNext()) {
                    pages.next().releaseBlocks();
                }
            }
        });
        assertThat(causeMessages(e), containsString("Failed to expand [items] at row group [2] page batch [1] in file ["));
        assertThat(causeMessages(e), containsString("injected"));
    }

    /**
     * An expanding read keeps the scan off two-phase I/O, which decodes a row group's predicate columns whole and so
     * could neither cut a batch at its budget nor decode the elements beside the rows. The positive control shows the
     * fixture does go two-phase without an expand read: from storage that advertises async reads it fetches fewer bytes
     * than from storage that does not, which only two-phase's skipped projection pages explain.
     */
    public void testExpandReadKeepsTwoPhaseOff() throws Exception {
        assumeTrue("two-phase I/O exists on the optimized iterator only", optimized);
        String padding = "x".repeat(1024);
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 6000; r++) {
                Group g = factory.newGroup().append("id", (long) r).append("label", padding + r);
                g.addGroup("items").addGroup("list").addGroup("element").append("pid", (long) r);
                groups.add(g);
            }
            return groups;
        });
        assertThat(data.length, greaterThan(ParquetStorageObjectAdapter.DEFAULT_WINDOW_SIZE));
        ReferenceAttribute id = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, id, new Literal(Source.EMPTY, 50L, DataType.LONG), null);
        UnaryOperator<ParquetFormatReader> pushed = r -> r.withPushedFilter(new ParquetPushedExpressions(List.of(filter)));

        FormatReadContext plain = FormatReadContext.builder().projectedColumns(List.of("id", "label")).batchSize(1024).build();
        AtomicLong localBytes = new AtomicLong();
        AtomicLong remoteBytes = new AtomicLong();
        drain(pushed.apply(reader()), storage(data, false, localBytes), plain);
        drain(pushed.apply(reader()), storage(data, true, remoteBytes), plain);
        assertThat("the fixture goes two-phase without an expand read", remoteBytes.get(), lessThan(localBytes.get()));

        Expanded read = read(
            data,
            context(List.of("id", "label"), leaf("pid", DataType.LONG)).batchSize(1024),
            pushed,
            storage(data, true, new AtomicLong())
        );
        assertThat(read.rows.size(), equalTo(50));
        for (List<Object> row : read.rows) {
            assertThat(row.get(2), equalTo(row.get(0)));
        }
    }

    /**
     * {@code readRange} expands as {@code read} does, and the parent ordinals an operator factory asks for end each
     * page, numbering its parents from {@code 0}.
     */
    public void testReadRangeExpandsWithParentOrdinals() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (long r = 0; r < 3; r++) {
                Group g = factory.newGroup().append("id", r);
                if (r != 1) {
                    Group items = g.addGroup("items");
                    items.addGroup("list").addGroup("element").append("pid", r);
                    items.addGroup("list").addGroup("element").append("pid", r);
                }
                groups.add(g);
            }
            return groups;
        });
        RangeReadContext range = new RangeReadContext(
            List.of("id"),
            1000,
            0,
            data.length,
            List.of(new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG)),
            ErrorPolicy.STRICT,
            null,
            FormatReader.NO_LIMIT,
            null,
            null,
            new ExpandRead("items", List.of(leaf("pid", DataType.LONG)))
        );
        ParquetFormatReader reader = reader();
        reader.clearFooterCachesForTests();
        List<List<Object>> rows = new ArrayList<>();
        try (CloseableIterator<Page> pages = reader.readRange(storage(data, false, new AtomicLong()), range)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        List<Object> row = new ArrayList<>();
                        for (int b = 0; b < page.getBlockCount(); b++) {
                            row.add(BlockUtils.toJavaObject(page.getBlock(b), p));
                        }
                        rows.add(row);
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        assertThat(
            rows,
            equalTo(
                List.of(
                    Arrays.asList(0L, 0L, 0),
                    Arrays.asList(0L, 0L, 0),
                    Arrays.asList(1L, null, 1),
                    Arrays.asList(2L, 2L, 2),
                    Arrays.asList(2L, 2L, 2)
                )
            )
        );
    }

    /**
     * Page-index row ranges skip pages of the parent columns, and the element leaves, repeated columns synchronised
     * through their offset index, skip with them, so every element row still sits beside its own parent.
     */
    public void testPageIndexRowRangesKeepElementsWithTheirParents() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (long r = 0; r < 40; r++) {
                Group g = factory.newGroup().append("id", r);
                Group items = g.addGroup("items");
                for (int e = 0; e < 1 + r % 3; e++) {
                    items.addGroup("list").addGroup("element").append("pid", r).append("sku", "s" + r);
                }
                groups.add(g);
            }
            return groups;
        }, 0, 4);
        ReferenceAttribute id = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new GreaterThanOrEqual(Source.EMPTY, id, new Literal(Source.EMPTY, 30L, DataType.LONG), null);
        Expanded read = read(
            data,
            context(List.of("id"), leaf("pid", DataType.LONG), leaf("sku", DataType.KEYWORD)).batchSize(8),
            r -> r.withPushedFilter(new ParquetPushedExpressions(List.of(filter)))
        );
        long expected = 0;
        for (long r = 30; r < 40; r++) {
            expected += 1 + r % 3;
        }
        for (List<Object> row : read.rows) {
            assertThat(row.get(1), equalTo(row.get(0)));
            assertThat(row.get(2), equalTo(ref("s" + row.get(0))));
        }
        if (optimized) {
            assertThat(read.rows.size(), equalTo((int) expected));
        } else {
            // The baseline iterator's record filter is parquet-mr's, which may leave rows for the FilterExec above
            assertThat(read.rows.size(), greaterThanOrEqualTo((int) expected));
            assertTrue(read.rows.stream().map(row -> (Long) row.get(0)).toList().containsAll(List.of(30L, 35L, 39L)));
        }
    }

    /**
     * Late materialization over a batch that keeps every parent, followed by one whose survivors interleave with
     * dropped parents: the survivors of each batch, and only they, are expanded, beside their own elements.
     */
    public void testLateMaterializationFullBatchThenInterleavedSurvivors() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (long r = 0; r < 20; r++) {
                Group g = factory.newGroup().append("id", r).append("n", r < 5 || r % 2 == 1 ? 1L : 0L);
                Group items = g.addGroup("items");
                items.addGroup("list").addGroup("element").append("pid", r);
                items.addGroup("list").addGroup("element").append("pid", r);
                groups.add(g);
            }
            return groups;
        });
        ReferenceAttribute n = new ReferenceAttribute(Source.EMPTY, "n", DataType.LONG);
        Expression filter = new Equals(Source.EMPTY, n, new Literal(Source.EMPTY, 1L, DataType.LONG), null);
        Expanded read = read(
            data,
            context(List.of("id", "n"), leaf("pid", DataType.LONG)).batchSize(10),
            r -> r.withPushedFilter(new ParquetPushedExpressions(List.of(filter)))
        );
        for (List<Object> row : read.rows) {
            assertThat(row.get(2), equalTo(row.get(0)));
        }
        List<Long> ids = new ArrayList<>();
        for (long r = 0; r < 20; r++) {
            if (r < 5 || r % 2 == 1) {
                ids.add(r);
                ids.add(r);
            }
        }
        List<Long> read0 = read.rows.stream().map(row -> (Long) row.get(0)).toList();
        if (optimized) {
            assertThat(read0, equalTo(ids));
        } else {
            // The baseline iterator leaves the filter to the plan above it, so it keeps at least the survivors
            assertTrue(read0.containsAll(ids));
        }
    }

    private ParquetFormatReader reader() {
        return new ParquetFormatReader(blockFactory, optimized);
    }

    private static ExpandRead.Leaf leaf(String name, DataType type) {
        return new ExpandRead.Leaf(name, type);
    }

    private static BytesRef ref(String value) {
        return new BytesRef(value);
    }

    private static ParquetExpansion.Spec spec(MessageType fileSchema, ExpandRead.Leaf... leaves) {
        return ParquetExpansion.Spec.of(
            fileSchema,
            ExternalSourceSettings.DEFAULT_SCHEMA_MAX_FIELDS,
            new ExpandRead("items", List.of(leaves)),
            1,
            1000,
            "memory://expand.parquet",
            ErrorPolicy.STRICT,
            null
        );
    }

    private static FormatReadContext.Builder context(List<String> projected, ExpandRead.Leaf... leaves) {
        return FormatReadContext.builder().projectedColumns(projected).batchSize(1000).expandRead(new ExpandRead("items", List.of(leaves)));
    }

    /** What a read returned: every row's values over every block, and per page its rows and blocks. */
    private static final class Expanded {
        final List<List<Object>> rows = new ArrayList<>();
        final List<Integer> pageRows = new ArrayList<>();
        final List<Integer> blockCounts = new ArrayList<>();
    }

    private Expanded read(byte[] data, FormatReadContext.Builder context) throws IOException {
        return read(data, context, UnaryOperator.identity());
    }

    private Expanded read(byte[] data, FormatReadContext.Builder context, UnaryOperator<ParquetFormatReader> customize) throws IOException {
        return read(data, context, customize, storage(data, false, new AtomicLong()));
    }

    private Expanded read(
        byte[] data,
        FormatReadContext.Builder context,
        UnaryOperator<ParquetFormatReader> customize,
        StorageObject storage
    ) throws IOException {
        ParquetFormatReader reader = customize.apply(reader());
        reader.clearFooterCachesForTests();
        Expanded read = new Expanded();
        try (CloseableIterator<Page> pages = reader.read(storage, context.build())) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    read.pageRows.add(page.getPositionCount());
                    // Every expanded page ends with the parent ordinals, which the rows below leave out
                    int columns = page.getBlockCount() - 1;
                    assertThat(page.getBlock(columns), instanceOf(IntBlock.class));
                    read.blockCounts.add(columns);
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        List<Object> row = new ArrayList<>();
                        for (int b = 0; b < columns; b++) {
                            Block block = page.getBlock(b);
                            row.add(BlockUtils.toJavaObject(block, p));
                        }
                        read.rows.add(row);
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return read;
    }

    private void drain(ParquetFormatReader reader, StorageObject storage, FormatReadContext context) throws IOException {
        reader.clearFooterCachesForTests();
        try (CloseableIterator<Page> pages = reader.read(storage, context)) {
            while (pages.hasNext()) {
                pages.next().releaseBlocks();
            }
        }
    }

    private static String causeMessages(Throwable t) {
        StringBuilder out = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            out.append(c.getMessage()).append('\n');
        }
        return out.toString();
    }

    private static ParquetStorageObjectAdapter storageAdapter(byte[] data) {
        return new ParquetStorageObjectAdapter(
            storage(data, false, new AtomicLong()),
            FooterByteCache.fromSettings(Settings.EMPTY),
            new NoopCircuitBreaker("test")
        );
    }

    private static byte[] write(MessageType schema, Function<SimpleGroupFactory, List<Group>> rows) throws IOException {
        return write(schema, rows, 0, 0);
    }

    /**
     * @param rowGroupRows rows per row group, or {@code 0} for one row group
     * @param pageRows     rows per page with dictionary encoding on, or {@code 0} for the writer's defaults
     */
    private static byte[] write(MessageType schema, Function<SimpleGroupFactory, List<Group>> rows, int rowGroupRows, int pageRows)
        throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outputFile(bytes))
            .withConf(new PlainParquetConfiguration())
            .withCodecFactory(new PlainCompressionCodecFactory())
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED);
        if (rowGroupRows > 0) {
            builder = builder.withRowGroupRowCountLimit(rowGroupRows);
        }
        if (pageRows > 0) {
            builder = builder.withPageRowCountLimit(pageRows).withDictionaryEncoding(true).withMinRowCountForPageSizeCheck(1);
        }
        try (ParquetWriter<Group> writer = builder.build()) {
            for (Group row : rows.apply(new SimpleGroupFactory(schema))) {
                writer.write(row);
            }
        }
        return bytes.toByteArray();
    }

    private static OutputFile outputFile(ByteArrayOutputStream bytes) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) {
                        bytes.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) {
                        bytes.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    /**
     * An in-memory file that counts the bytes read from it; {@code nativeAsync} makes it look remote, which is what lets
     * a scan go two-phase.
     */
    private static StorageObject storage(byte[] data, boolean nativeAsync, AtomicLong bytesRead) {
        return new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                bytesRead.addAndGet(data.length);
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int len = (int) Math.min(length, data.length - position);
                bytesRead.addAndGet(len);
                return new ByteArrayInputStream(data, (int) position, len);
            }

            @Override
            public long length() {
                return data.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://expand.parquet");
            }

            @Override
            public boolean supportsNativeAsync() {
                return nativeAsync;
            }

            @Override
            public void readBytesAsync(
                long position,
                long length,
                DirectBufferFactory factory,
                Executor executor,
                ActionListener<DirectReadBuffer> listener
            ) {
                try (InputStream stream = newStream(position, length)) {
                    listener.onResponse(new DirectReadBuffer(ByteBuffer.wrap(stream.readAllBytes()), () -> {}));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            }
        };
    }
}
