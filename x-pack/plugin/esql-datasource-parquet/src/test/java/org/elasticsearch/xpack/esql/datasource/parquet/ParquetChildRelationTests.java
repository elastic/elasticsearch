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
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ChildRelationPages;
import org.elasticsearch.xpack.esql.datasources.spi.ChildRelationRead;
import org.elasticsearch.xpack.esql.datasources.spi.ChildWindows;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;

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
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The element pages a Parquet read decodes beside its rows for a list of structures, on the optimized iterator and on
 * the baseline one, which must agree. Every element carries its parent's id in a {@code pid} leaf where a test needs to
 * prove that an element's parent position points at the right row of the page it came with.
 */
public class ParquetChildRelationTests extends ESTestCase {

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("""
        message t {
          required int64 id;
          optional binary label (UTF8);
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

    private final boolean optimized;
    private BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    public ParquetChildRelationTests(@Name("optimized") boolean optimized) {
        this.optimized = optimized;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    /**
     * Two leaves line up element by element, a null field value staying a null on its own element, and a list inside the
     * element is that leaf's multivalue.
     */
    public void testChildWindowsAligned() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items0 = r0.addGroup("items");
            Group e0 = items0.addGroup("list").addGroup("element").append("sku", "a").append("qty", 1);
            e0.addGroup("tags").addGroup("list").append("element", "x");
            items0.addGroup("list").addGroup("element").append("qty", 2);
            Group e2 = items0.addGroup("list").addGroup("element").append("sku", "c");
            Group tags2 = e2.addGroup("tags");
            tags2.addGroup("list").append("element", "y");
            tags2.addGroup("list").append("element", "z");
            Group r1 = factory.newGroup().append("id", 1L);
            r1.addGroup("items").addGroup("list").addGroup("element").append("sku", "d").append("qty", 4);
            return List.of(r0, r1);
        });
        Read read = read(data, null, leaf("sku", DataType.KEYWORD), leaf("qty", DataType.INTEGER), leaf("tags", DataType.KEYWORD));
        assertThat(read.parents, equalTo(List.of(0L, 1L)));
        assertThat(
            read.elements,
            equalTo(
                List.of(
                    Arrays.asList(0L, new BytesRef("a"), 1, new BytesRef("x")),
                    Arrays.asList(0L, null, 2, null),
                    Arrays.asList(0L, new BytesRef("c"), null, List.of(new BytesRef("y"), new BytesRef("z"))),
                    Arrays.asList(1L, new BytesRef("d"), 4, null)
                )
            )
        );
    }

    /**
     * A batch ends at the row where its elements reach the budget, so no batch holds much more than the budget, and the
     * rows and elements over the whole read are unchanged. A single parent larger than the budget is held whole.
     */
    public void testParentBatchCutAtElementBudget() throws Exception {
        int rows = 200;
        int perRow = 700;
        int huge = ParquetChildRelations.BATCH_ELEMENT_BUDGET + 10;
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r <= rows; r++) {
                Group g = factory.newGroup().append("id", (long) r);
                Group items = g.addGroup("items");
                int count = r == rows ? huge : perRow;
                for (int e = 0; e < count; e++) {
                    items.addGroup("list").addGroup("element").append("pid", (long) r);
                }
                groups.add(g);
            }
            return groups;
        });
        Read read = read(data, null, leaf("pid", DataType.LONG));
        assertThat(read.parents.size(), equalTo(rows + 1));
        assertThat(read.elements.size(), equalTo(rows * perRow + huge));
        assertThat("a batch of " + (rows + 1) + " rows must have been cut", read.pageRows.size(), greaterThan(2));
        for (int p = 0; p < read.pageRows.size(); p++) {
            int pageRows = read.pageRows.get(p);
            long elements = read.pageElements.get(p);
            assertThat(pageRows, greaterThanOrEqualTo(1));
            // The batch stops at the row that reaches the budget: without its last row it stayed under it
            long lastRow = p == read.pageRows.size() - 1 ? huge : perRow;
            assertThat(elements - lastRow, lessThan((long) ParquetChildRelations.BATCH_ELEMENT_BUDGET));
        }
        assertThat("the parent above the budget is held whole", read.pageElements.getLast(), greaterThanOrEqualTo((long) huge));
        assertThat(read.maxWindowPageElements, lessThanOrEqualTo(ChildWindows.MAX_ELEMENTS_PER_PAGE));
        for (List<Object> element : read.elements) {
            assertThat(element.get(1), equalTo(element.get(0)));
        }
    }

    /**
     * A pushed filter that drops parents in the middle of a batch takes their elements with them, and every surviving
     * element still points at its own parent's row.
     */
    public void testChildWindowsFollowSurvivors() throws Exception {
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
        Expression filter = new GreaterThan(Source.EMPTY, id, new Literal(Source.EMPTY, 6L, DataType.LONG), null);
        Read read = read(data, new ParquetPushedExpressions(List.of(filter)), leaf("pid", DataType.LONG), leaf("sku", DataType.KEYWORD));
        if (optimized) {
            // The optimized iterator evaluates the filter itself; the baseline one leaves the rows to a filter above it
            assertThat(read.parents, equalTo(List.of(7L, 8L, 9L)));
        }
        for (List<Object> element : read.elements) {
            assertThat(element.get(1), equalTo(element.get(0)));
        }
        long expected = read.parents.stream().mapToLong(p -> p % 3 + 1).sum();
        assertThat((long) read.elements.size(), equalTo(expected));
    }

    /**
     * A leaf the file lacks is null on every element, beside a leaf it has; asked for alone, the elements are still
     * there, found from another leaf's levels.
     */
    public void testChildLeafMissingInFileIsNull() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("sku", "a");
            items.addGroup("list").addGroup("element").append("qty", 3);
            return List.of(r0);
        });
        Read both = read(data, null, leaf("sku", DataType.KEYWORD), leaf("color", DataType.KEYWORD));
        assertThat(both.elements, equalTo(List.of(Arrays.asList(0L, new BytesRef("a"), null), Arrays.asList(0L, null, null))));
        Read alone = read(data, null, leaf("color", DataType.KEYWORD));
        assertThat(alone.elements, equalTo(List.of(Arrays.asList(0L, (Object) null), Arrays.asList(0L, (Object) null))));
    }

    /**
     * A leaf the file stores as another type is converted to the requested one when that conversion exists, and refused
     * naming both types when it does not.
     */
    public void testChildLeafCastToRequestedType() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("qty", 7).append("pid", 0L);
            items.addGroup("list").addGroup("element").append("pid", 0L);
            return List.of(r0);
        });
        Read read = read(data, null, leaf("qty", DataType.LONG));
        assertThat(read.elements, equalTo(List.of(Arrays.asList(0L, 7L), Arrays.asList(0L, (Object) null))));
        assertThat(read.leafBlockTypes, equalTo(List.of(LongBlock.class)));

        Exception e = expectThrows(Exception.class, () -> read(data, null, leaf("pid", DataType.BOOLEAN)));
        assertThat(
            causeMessages(e),
            containsString("[items.pid] is [long] in this file but [boolean] in the dataset, and an element's field cannot be converted")
        );
    }

    /** The projection holds the requested leaves of the element and none of its other leaves. */
    public void testOnlyRequestedLeavesProjected() {
        List<Attribute> id = List.of(new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG));
        MessageType projected = ParquetFormatReader.projectedSchema(
            SCHEMA,
            id,
            Map.of(),
            List.of(new ChildRelationRead("items", List.of(leaf("sku", DataType.KEYWORD), leaf("tags", DataType.KEYWORD))))
        );
        assertThat(
            projected.getColumns().stream().map(c -> String.join(".", c.getPath())).toList(),
            equalTo(List.of("id", "items.list.element.sku", "items.list.element.tags.list.element"))
        );
        MessageType structural = ParquetFormatReader.projectedSchema(
            SCHEMA,
            id,
            Map.of(),
            List.of(new ChildRelationRead("items", List.of(leaf("color", DataType.KEYWORD))))
        );
        assertThat(
            structural.getColumns().stream().map(c -> String.join(".", c.getPath())).toList(),
            equalTo(List.of("id", "items.list.element.pid"))
        );
    }

    /** A null element is dropped and announced with the warning a list column's null elements raise. */
    public void testNullElementDroppedWithWarning() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            Group items = r0.addGroup("items");
            items.addGroup("list").addGroup("element").append("sku", "a");
            items.addGroup("list");
            items.addGroup("list").addGroup("element").append("sku", "c");
            return List.of(r0);
        });
        List<String> warnings = Collections.synchronizedList(new ArrayList<>());
        Read read = read(data, null, warnings::add, leaf("sku", DataType.KEYWORD));
        assertThat(read.elements, equalTo(List.of(Arrays.asList(0L, new BytesRef("a")), Arrays.asList(0L, new BytesRef("c")))));
        assertThat(String.join("\n", warnings), containsString(ParquetColumnDecoding.nullListElementsMessage("items")));
    }

    /** An empty list and a missing one give their parent no elements. */
    public void testEmptyAndNullListHaveNoElements() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            Group r0 = factory.newGroup().append("id", 0L);
            r0.addGroup("items");
            Group r1 = factory.newGroup().append("id", 1L);
            Group r2 = factory.newGroup().append("id", 2L);
            r2.addGroup("items").addGroup("list").addGroup("element").append("sku", "z");
            return List.of(r0, r1, r2);
        });
        Read read = read(data, null, leaf("sku", DataType.KEYWORD));
        assertThat(read.parents, equalTo(List.of(0L, 1L, 2L)));
        assertThat(read.elements, equalTo(List.of(Arrays.asList(2L, new BytesRef("z")))));
    }

    /**
     * Every element block is charged to the read's breaker and released by the time the read is closed, whether the
     * consumer released the windows itself or left them to the next page and the close.
     */
    public void testChildWindowsReleasedToBreaker() throws Exception {
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 50; r++) {
                Group g = factory.newGroup().append("id", (long) r);
                Group items = g.addGroup("items");
                for (int e = 0; e < 300; e++) {
                    items.addGroup("list").addGroup("element").append("pid", (long) r).append("sku", "s" + e);
                }
                groups.add(g);
            }
            return groups;
        });
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(256));
        blockFactory = new BlockFactory(breaker, BigArrays.NON_RECYCLING_INSTANCE);
        ParquetFormatReader reader = reader();
        reader.clearFooterCachesForTests();
        FormatReadContext context = FormatReadContext.builder()
            .projectedColumns(List.of("id"))
            .batchSize(20)
            .childRelations(List.of(new ChildRelationRead("items", List.of(leaf("pid", DataType.LONG), leaf("sku", DataType.KEYWORD)))))
            .build();
        long peak = 0;
        try (CloseableIterator<Page> pages = reader.read(storage(data), context)) {
            int page = 0;
            while (pages.hasNext()) {
                Page parents = pages.next();
                ChildWindows windows = ((ChildRelationPages) pages).children("items");
                peak = Math.max(peak, breaker.getUsed());
                assertThat(windows.elementCount(), greaterThan(0L));
                if (page++ % 2 == 0) {
                    windows.close();
                    assertTrue(windows.isReleased());
                }
                parents.releaseBlocks();
            }
        }
        assertThat("element blocks are charged to the breaker", peak, greaterThan(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Reading child relations keeps the scan off two-phase I/O, which decodes a row group's predicate columns whole and
     * so could neither cut a batch at the element budget nor decode the elements beside the rows. The fixture would go
     * two-phase without them: a selective filter on a small column, a large projection-only column, remote storage.
     */
    public void testChildRelationsKeepTwoPhaseOff() throws Exception {
        assumeTrue("two-phase I/O exists on the optimized iterator only", optimized);
        String padding = "x".repeat(256);
        byte[] data = write(SCHEMA, factory -> {
            List<Group> groups = new ArrayList<>();
            for (int r = 0; r < 5000; r++) {
                Group g = factory.newGroup().append("id", (long) r).append("label", padding + r);
                g.addGroup("items").addGroup("list").addGroup("element").append("pid", (long) r);
                groups.add(g);
            }
            return groups;
        });
        ReferenceAttribute id = new ReferenceAttribute(Source.EMPTY, "id", DataType.LONG);
        Expression filter = new LessThan(Source.EMPTY, id, new Literal(Source.EMPTY, 50L, DataType.LONG), null);
        ParquetFormatReader reader = reader().withPushedFilter(new ParquetPushedExpressions(List.of(filter)));
        FormatReadContext context = FormatReadContext.builder()
            .projectedColumns(List.of("id", "label"))
            .batchSize(1024)
            .childRelations(List.of(new ChildRelationRead("items", List.of(leaf("pid", DataType.LONG)))))
            .build();
        List<Long> parents = new ArrayList<>();
        List<Long> pids = new ArrayList<>();
        try (CloseableIterator<Page> pages = reader.read(storage(data, true), context)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    LongBlock ids = page.getBlock(0);
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        parents.add(ids.getLong(p));
                    }
                    ChildWindows windowsOfPage = ((ChildRelationPages) pages).children("items");
                    while (windowsOfPage.hasNext()) {
                        Page elements = windowsOfPage.next();
                        try {
                            IntBlock parent = elements.getBlock(0);
                            LongBlock pid = elements.getBlock(1);
                            for (int e = 0; e < elements.getPositionCount(); e++) {
                                assertThat(pid.getLong(e), equalTo(ids.getLong(parent.getInt(e))));
                                pids.add(pid.getLong(e));
                            }

                        } finally {
                            elements.releaseBlocks();
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        assertThat(parents.size(), equalTo(50));
        assertThat(pids, equalTo(parents));
    }

    private ParquetFormatReader reader() {
        return new ParquetFormatReader(blockFactory, optimized);
    }

    private static ChildRelationRead.Leaf leaf(String name, DataType type) {
        return new ChildRelationRead.Leaf(name, type);
    }

    private static FormatReadContext context(ChildRelationRead.Leaf... leaves) {
        return FormatReadContext.builder()
            .projectedColumns(List.of("id"))
            .batchSize(1000)
            .childRelations(List.of(new ChildRelationRead("items", List.of(leaves))))
            .build();
    }

    /** What a read returned: parent ids, and per element its parent's id followed by its leaves' values. */
    private static final class Read {
        final List<Long> parents = new ArrayList<>();
        final List<List<Object>> elements = new ArrayList<>();
        final List<Integer> pageRows = new ArrayList<>();
        final List<Long> pageElements = new ArrayList<>();
        final List<Class<?>> leafBlockTypes = new ArrayList<>();
        int maxWindowPageElements;
    }

    private Read read(byte[] data, ParquetPushedExpressions pushed, ChildRelationRead.Leaf... leaves) throws IOException {
        return read(data, pushed, null, leaves);
    }

    private Read read(byte[] data, ParquetPushedExpressions pushed, Consumer<String> warnings, ChildRelationRead.Leaf... leaves)
        throws IOException {
        ParquetFormatReader reader = reader();
        if (pushed != null) {
            reader = reader.withPushedFilter(pushed);
        }
        FormatReadContext context = FormatReadContext.builder()
            .projectedColumns(List.of("id"))
            .batchSize(1000)
            .informationalWarningSink(warnings)
            .childRelations(List.of(new ChildRelationRead("items", List.of(leaves))))
            .build();
        Read read = new Read();
        try (CloseableIterator<Page> pages = reader.read(storage(data), context)) {
            while (pages.hasNext()) {
                Page page = pages.next();
                try {
                    LongBlock ids = page.getBlock(0);
                    for (int p = 0; p < page.getPositionCount(); p++) {
                        read.parents.add(ids.getLong(p));
                    }
                    ChildWindows windows = ((ChildRelationPages) pages).children("items");
                    assertThat(windows.parentCount(), equalTo(page.getPositionCount()));
                    read.pageRows.add(page.getPositionCount());
                    read.pageElements.add(windows.elementCount());
                    int previous = -1;
                    while (windows.hasNext()) {
                        Page elements = windows.next();
                        try {
                            read.maxWindowPageElements = Math.max(read.maxWindowPageElements, elements.getPositionCount());
                            IntBlock parent = elements.getBlock(0);
                            assertNotNull("parent positions are a vector", parent.asVector());
                            for (int e = 0; e < elements.getPositionCount(); e++) {
                                int position = parent.getInt(e);
                                assertThat(position, greaterThanOrEqualTo(previous));
                                previous = position;
                                List<Object> element = new ArrayList<>();
                                element.add(ids.getLong(position));
                                for (int b = 1; b < elements.getBlockCount(); b++) {
                                    element.add(BlockUtils.toJavaObject(elements.getBlock(b), e));
                                }
                                read.elements.add(element);
                            }
                            if (read.leafBlockTypes.isEmpty()) {
                                for (int b = 1; b < elements.getBlockCount(); b++) {
                                    Block block = elements.getBlock(b);
                                    read.leafBlockTypes.add(block instanceof LongBlock ? LongBlock.class : block.getClass());
                                }
                            }

                        } finally {
                            elements.releaseBlocks();
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            }
        }
        return read;
    }

    private static String causeMessages(Throwable t) {
        StringBuilder out = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            out.append(c.getMessage()).append('\n');
        }
        return out.toString();
    }

    private static byte[] write(MessageType schema, Function<SimpleGroupFactory, List<Group>> rows) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile(bytes))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
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

    private static StorageObject storage(byte[] data) {
        return storage(data, false);
    }

    /** An in-memory file; {@code nativeAsync} makes it look remote, which is what lets a scan go two-phase. */
    private static StorageObject storage(byte[] data, boolean nativeAsync) {
        return new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) Math.min(length, data.length - position));
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
                return StoragePath.of("memory://child-relations.parquet");
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
