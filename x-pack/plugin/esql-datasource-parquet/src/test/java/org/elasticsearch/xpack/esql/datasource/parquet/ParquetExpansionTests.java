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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;

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

    /** One leaf per value conversion an element leaf can need. */
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
        Expanded alone = read(data, context(List.of("id"), leaf("color", DataType.KEYWORD)));
        assertThat(alone.rows, equalTo(List.of(Arrays.asList(0L, null), Arrays.asList(0L, null), Arrays.asList(1L, null))));
    }

    /** A file without the collection keeps every parent as one null row. */
    public void testFileWithoutCollection() throws Exception {
        MessageType flat = MessageTypeParser.parseMessageType("message t { required int64 id; }");
        byte[] data = write(flat, factory -> List.of(factory.newGroup().append("id", 0L), factory.newGroup().append("id", 1L)));
        Expanded read = read(data, context(List.of("id"), leaf("sku", DataType.KEYWORD)));
        assertThat(read.rows, equalTo(List.of(Arrays.asList(0L, null), Arrays.asList(1L, null))));
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
     * An element value that cannot be converted is nulled under a lenient policy and its row kept, and the warning says
     * so rather than that rows were skipped; a strict policy fails the read.
     */
    public void testCastFailureNullsTheValueTruthfully() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("pid", 1L << 40);
            items.addGroup("list").addGroup("element").append("pid", 5L);
            return List.of(r0);
        });
        for (ErrorPolicy.Mode mode : List.of(ErrorPolicy.Mode.SKIP_ROW, ErrorPolicy.Mode.NULL_FIELD)) {
            List<String> warnings = new ArrayList<>();
            Expanded read = read(
                data,
                context(List.of("id"), leaf("pid", DataType.INTEGER)).errorPolicy(new ErrorPolicy(mode, Long.MAX_VALUE, 0.0, false))
                    .informationalWarningSink(warnings::add)
            );
            assertThat(mode.toString(), read.rows, equalTo(List.of(Arrays.asList(0L, null), Arrays.asList(0L, 5))));
            assertThat(warnings, hasItem(containsString("cannot be read as their declared type; returning null")));
            for (String warning : warnings) {
                assertFalse(warning, warning.contains("skipping"));
            }
        }
        Exception e = expectThrows(
            Exception.class,
            () -> read(data, context(List.of("id"), leaf("pid", DataType.INTEGER)).errorPolicy(ErrorPolicy.STRICT))
        );
        assertThat(causeMessages(e), containsString("out of range for an integer"));
    }

    /**
     * A leaf whose type in the file cannot be converted at all is decided as a declared column is: {@code fail_fast}
     * fails, {@code null_field} reads it as null, and {@code skip_row} drops every row of the file.
     */
    public void testUnconvertibleLeafFollowsTheColumnPrecedent() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            r0.addGroup("items").addGroup("list").addGroup("element").append("pid", 0L).append("sku", "a");
            return List.of(r0, factory.newGroup().append("id", 1L));
        });
        ExpandRead.Leaf bad = leaf("pid", DataType.BOOLEAN);
        Exception e = expectThrows(Exception.class, () -> read(data, context(List.of("id"), bad).errorPolicy(ErrorPolicy.STRICT)));
        assertThat(causeMessages(e), containsString("column [items.pid] in ["));
        assertThat(causeMessages(e), containsString("is [long] in the file and cannot be read as its declared type [boolean]"));

        List<String> nullWarnings = new ArrayList<>();
        Expanded nulled = read(
            data,
            context(List.of("id"), bad, leaf("sku", DataType.KEYWORD)).errorPolicy(ErrorPolicy.PERMISSIVE)
                .informationalWarningSink(nullWarnings::add)
        );
        assertThat(nulled.rows, equalTo(List.of(Arrays.asList(0L, null, ref("a")), Arrays.asList(1L, null, null))));
        assertThat(nullWarnings, hasItem(containsString("column [items.pid]: [long] in the file, [boolean] in the query")));

        List<String> dropWarnings = new ArrayList<>();
        Expanded dropped = read(
            data,
            context(List.of("id"), bad).errorPolicy(new ErrorPolicy(ErrorPolicy.Mode.SKIP_ROW, 10, 0.0, false))
                .informationalWarningSink(dropWarnings::add)
        );
        assertThat(dropped.rows, equalTo(List.of()));
        assertThat(dropWarnings, hasItem(containsString("skipping the file's rows")));
    }

    /** The projection holds the requested leaves of the element and none of its other leaves. */
    public void testOnlyRequestedLeavesProjected() {
        List<Attribute> id = List.of(new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG));
        MessageType projected = ParquetFormatReader.projectedSchema(
            SCHEMA,
            id,
            Map.of(),
            spec(SCHEMA, leaf("sku", DataType.KEYWORD), leaf("tags", DataType.KEYWORD))
        );
        assertThat(
            projected.getColumns().stream().map(c -> String.join(".", c.getPath())).toList(),
            equalTo(List.of("id", "items.list.element.sku", "items.list.element.tags.list.element"))
        );
        MessageType structural = ParquetFormatReader.projectedSchema(SCHEMA, id, Map.of(), spec(SCHEMA, leaf("color", DataType.KEYWORD)));
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
            ParquetFormatReader.projectedSchema(SCHEMA, List.of(), Map.of(), spec(SCHEMA, leaf("pid", DataType.LONG)))
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
        if (optimized) {
            assertThat(parents, equalTo(List.of(0L, 1L, 2L, 3L, 5L, 6L, 7L, 8L, 9L)));
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

    /** A row position addresses a file row, so a read that expands refuses to carry one. */
    public void testRowPositionsRefused() throws Exception {
        byte[] data = write(SCHEMA, factory -> List.of(factory.newGroup().append("id", 0L)));
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
        assertNotNull(data);
    }

    /**
     * Every block is charged to the read's breaker and released by the time the read is closed, whether the read
     * finished or a breaker trip at any point of it failed it. The limits span reads that cannot start to reads that
     * finish, so trips land in the decode, the parent columns and the join.
     */
    public void testBreakerReleasedOnSuccessAndFailure() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
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
        int tripped = 0;
        int finished = 0;
        for (long limit = 1024; limit <= 8 * 1024 * 1024; limit *= 2) {
            LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofBytes(limit));
            blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
            try {
                Expanded read = read(
                    data,
                    context(List.of("id", "label"), leaf("pid", DataType.LONG), leaf("sku", DataType.KEYWORD), leaf("qty", DataType.LONG))
                        .batchSize(64)
                );
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

    /** Every conversion from a leaf's physical value to its type: boolean, date, unsigned int, micros, float and double. */
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
                    read.blockCounts.add(page.getBlockCount());
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        List<Object> row = new ArrayList<>();
                        for (int b = 0; b < page.getBlockCount(); b++) {
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
