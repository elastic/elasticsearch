/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.FooterByteCache;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Step 1 of the composite-assembly prototype: establishes a Parquet file whose contents the test knows
 * independently, and an oracle built from parquet-mr's own record assembly, then checks the two agree.
 * <p>
 * Nothing in the production reader is exercised here on purpose. Later steps judge an assembler against
 * this oracle, so the oracle has to be trustworthy first — otherwise a disagreement later cannot be
 * attributed to the thing under test.
 * <p>
 * The small page size is the point rather than an economy: {@code sku} (strings) and {@code price}
 * (longs) fill pages at different rates, so their page boundaries fall on different rows. That is the
 * case parquet-mr's synchronizing column reader exists for, and the case most likely to break an
 * assembler that assumes one leaf per column.
 */
public class CompositeAssemblyTests extends ESTestCase {

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType("""
        message composite_assembly {
          required int64 id;
          optional int32 qty;
          optional group tags (LIST) { repeated group list { optional binary element (STRING); } }
          optional group items (LIST) {
            repeated group list {
              optional group element {
                optional binary sku (STRING);
                optional int64 price;
              }
            }
          }
          optional group attrs (MAP) {
            repeated group key_value {
              required binary key (STRING);
              optional int64 value;
            }
          }
        }
        """);

    private static final int ROWS = 5000;

    private final CompressionCodecName codec;
    private final WriterVersion writerVersion;

    public CompositeAssemblyTests(String description, CompressionCodecName codec, WriterVersion writerVersion) {
        this.codec = codec;
        this.writerVersion = writerVersion;
    }

    @ParametersFactory(argumentFormatting = "%1$s")
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();
        for (CompressionCodecName c : List.of(CompressionCodecName.UNCOMPRESSED, CompressionCodecName.SNAPPY)) {
            for (WriterVersion v : List.of(WriterVersion.PARQUET_1_0, WriterVersion.PARQUET_2_0)) {
                params.add(new Object[] { c + "/" + v, c, v });
            }
        }
        return params;
    }

    /**
     * One element of {@code items}. A null field means the field is absent from that element, which the
     * JSON conventions render as {@code "sku":null} rather than by omitting the key.
     */
    private record Element(String sku, Long price) {}

    /** One entry of {@code attrs}. A null value means the entry's {@code value} is absent. */
    private record Entry(String key, Long value) {}

    /**
     * The content of row {@code i}, as a pure function of {@code i}. The writer and the expectation both
     * read from this, so they cannot drift apart: a change to the shape changes both at once.
     * <p>
     * A null list means the whole group is absent (a null position). An empty list means the group is
     * present with no repeated child (an empty list or map). A null entry within {@code items} means the
     * repeated child carries no {@code element} group.
     */
    private record RowSpec(List<Element> items, List<Entry> attrs) {}

    private static RowSpec rowSpec(int i) {
        return switch (i % 8) {
            case 0 -> new RowSpec(
                List.of(new Element("sku-" + i, (long) i), new Element("sku-" + i + "b", (long) i + 1)),
                List.of(new Entry("a" + i, (long) i), new Entry("b" + i, (long) i + 1))
            );
            case 1 -> new RowSpec(null, null);
            case 2 -> new RowSpec(List.of(), List.of());
            case 3 -> new RowSpec(listWithNullElement(), List.of(new Entry("k" + i, null)));
            case 4 -> new RowSpec(
                List.of(new Element(null, (long) i)),
                List.of(new Entry("a" + i, (long) i), new Entry("b" + i, (long) i + 1), new Entry("c" + i, (long) i + 2))
            );
            case 5 -> new RowSpec(List.of(new Element("sku-" + i, null)), List.of(new Entry("only" + i, (long) i)));
            case 6 -> new RowSpec(List.of(new Element(null, null)), List.of(new Entry("a" + i, (long) i), new Entry("b" + i, null)));
            case 7 -> new RowSpec(mixedElements(i), entries(i % 4, i));
            default -> throw new AssertionError("i % 8 is exhaustive above: " + i);
        };
    }

    /** A single repeated child carrying no {@code element} group, which reads back as a null list entry. */
    private static List<Element> listWithNullElement() {
        List<Element> out = new ArrayList<>();
        out.add(null);
        return out;
    }

    private static List<Element> mixedElements(int i) {
        int n = 1 + i % 5;
        List<Element> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            if ((i + k) % 3 == 0) {
                out.add(new Element("sku-" + i + "-" + k, null));
            } else if ((i + k) % 3 == 1) {
                out.add(new Element(null, (long) (i * 10 + k)));
            } else {
                out.add(new Element("sku-" + i + "-" + k, (long) (i * 10 + k)));
            }
        }
        return out;
    }

    private static List<Entry> entries(int n, int i) {
        List<Entry> out = new ArrayList<>(n);
        for (int k = 0; k < n; k++) {
            out.add(new Entry("k" + i + "-" + k, (i + k) % 2 == 0 ? (long) (i + k) : null));
        }
        return out;
    }

    private static Integer qty(int i) {
        return i % 11 == 0 ? null : i % 100;
    }

    // --- expectations, independent of parquet-mr -------------------------------------------------

    /**
     * The JSON conventions, fixed here and shared by the oracle rendering and, in later steps, by the
     * assembler under test. All three must agree or the comparison means nothing: compact with no
     * whitespace; struct fields in schema order; an absent field written as {@code "field":null}; a null
     * list element written as {@code null}; an empty list {@code []}; an empty map <code>{}</code>; an
     * absent group is a null position rather than the text {@code null}; map entries in file order; a
     * null map value written as {@code "key":null}.
     */
    private static String expectedItemsJson(int i) {
        List<Element> items = rowSpec(i).items();
        if (items == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder("[");
        for (int k = 0; k < items.size(); k++) {
            if (k > 0) {
                sb.append(',');
            }
            Element e = items.get(k);
            if (e == null) {
                sb.append("null");
            } else {
                sb.append("{\"sku\":")
                    .append(quoteOrNull(e.sku()))
                    .append(",\"price\":")
                    .append(e.price() == null ? "null" : e.price())
                    .append('}');
            }
        }
        return sb.append(']').toString();
    }

    private static String expectedAttrsJson(int i) {
        List<Entry> attrs = rowSpec(i).attrs();
        if (attrs == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder("{");
        for (int k = 0; k < attrs.size(); k++) {
            if (k > 0) {
                sb.append(',');
            }
            Entry e = attrs.get(k);
            sb.append(quoteOrNull(e.key())).append(':').append(e.value() == null ? "null" : e.value());
        }
        return sb.append('}').toString();
    }

    /** The control column: a list of primitives, always present, with one to three entries. */
    private static String expectedTagsJson(int i) {
        StringBuilder sb = new StringBuilder("[");
        for (int k = 0; k <= i % 3; k++) {
            if (k > 0) {
                sb.append(',');
            }
            sb.append(quoteOrNull("t" + i + "-" + k));
        }
        return sb.append(']').toString();
    }

    private static String quoteOrNull(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(s.length() + 2).append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.append('"').toString();
    }

    // --- the file -------------------------------------------------------------------------------

    private byte[] writeFile() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(createOutputFile(outputStream))
                .withConf(new PlainParquetConfiguration())
                .withCodecFactory(new PlainCompressionCodecFactory())
                .withType(SCHEMA)
                .withCompressionCodec(codec)
                .withWriterVersion(writerVersion)
                .withRowGroupSize(64 * 1024L)
                .withPageSize(512)
                // Without these, parquet-mr checks page size every 100 records and, at a 512-byte
                // threshold, every column overflows at every checkpoint -- so all leaves break pages on
                // exactly the same rows and nothing has to be synchronized. Checking after each record
                // lets each leaf break where its own bytes run out, which is the case under test.
                .withMinRowCountForPageSizeCheck(1)
                .withMaxRowCountForPageSizeCheck(1)
                .build()
        ) {
            for (int i = 0; i < ROWS; i++) {
                writer.write(buildRow(groupFactory, i));
            }
        }
        return outputStream.toByteArray();
    }

    private static Group buildRow(SimpleGroupFactory groupFactory, int i) {
        Group g = groupFactory.newGroup();
        g.append("id", (long) i);
        Integer q = qty(i);
        if (q != null) {
            g.append("qty", q);
        }
        // tags is the control column: a list of primitives, which the reader supports today.
        Group tags = g.addGroup("tags");
        for (int k = 0; k <= i % 3; k++) {
            tags.addGroup("list").append("element", "t" + i + "-" + k);
        }
        RowSpec spec = rowSpec(i);
        if (spec.items() != null) {
            Group items = g.addGroup("items");
            for (Element e : spec.items()) {
                Group entry = items.addGroup("list");
                if (e != null) {
                    Group element = entry.addGroup("element");
                    if (e.sku() != null) {
                        element.append("sku", e.sku());
                    }
                    if (e.price() != null) {
                        element.append("price", e.price());
                    }
                }
            }
        }
        if (spec.attrs() != null) {
            Group attrs = g.addGroup("attrs");
            for (Entry e : spec.attrs()) {
                Group kv = attrs.addGroup("key_value").append("key", e.key());
                if (e.value() != null) {
                    kv.append("value", e.value());
                }
            }
        }
        return g;
    }

    // --- the oracle: parquet-mr's own record assembly ------------------------------------------

    /**
     * Reads every row through parquet-mr's own record assembly, which is the behaviour an assembler built
     * on the same machinery has to reproduce. Returns one group per row, in file order.
     */
    private List<Group> oracleRows(byte[] file) throws IOException {
        List<Group> rows = new ArrayList<>(ROWS);
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(
                    createStorageObject(file),
                    FooterByteCache.fromSettings(Settings.EMPTY),
                    new NoopCircuitBreaker("composite-assembly-test")
                ),
                PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
            )
        ) {
            MessageType schema = reader.getFileMetaData().getSchema();
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                RecordReader<Group> records = new ColumnIOFactory().getColumnIO(schema)
                    .getRecordReader(pages, new GroupRecordConverter(schema));
                for (long r = 0; r < pages.getRowCount(); r++) {
                    rows.add(records.read());
                }
            }
        }
        return rows;
    }

    /**
     * Renders one row's {@code items} or {@code attrs} from an assembled group, in the JSON conventions
     * fixed above. Returns null when the group is absent, which is a null position rather than text.
     */
    private static String groupToJson(Group row, String field) {
        if (row.getFieldRepetitionCount(field) == 0) {
            return null;
        }
        Group container = row.getGroup(field, 0);
        return field.equals("attrs") ? mapToJson(container) : listToJson(container);
    }

    private static String listToJson(Group list) {
        int n = list.getFieldRepetitionCount("list");
        StringBuilder sb = new StringBuilder("[");
        for (int k = 0; k < n; k++) {
            if (k > 0) {
                sb.append(',');
            }
            Group entry = list.getGroup("list", k);
            if (entry.getFieldRepetitionCount("element") == 0) {
                sb.append("null");
            } else {
                Group element = entry.getGroup("element", 0);
                String sku = element.getFieldRepetitionCount("sku") == 0 ? null : element.getBinary("sku", 0).toStringUsingUTF8();
                String price = element.getFieldRepetitionCount("price") == 0 ? "null" : Long.toString(element.getLong("price", 0));
                sb.append("{\"sku\":").append(quoteOrNull(sku)).append(",\"price\":").append(price).append('}');
            }
        }
        return sb.append(']').toString();
    }

    private static String mapToJson(Group map) {
        int n = map.getFieldRepetitionCount("key_value");
        StringBuilder sb = new StringBuilder("{");
        for (int k = 0; k < n; k++) {
            if (k > 0) {
                sb.append(',');
            }
            Group kv = map.getGroup("key_value", k);
            String key = kv.getBinary("key", 0).toStringUsingUTF8();
            String value = kv.getFieldRepetitionCount("value") == 0 ? "null" : Long.toString(kv.getLong("value", 0));
            sb.append(quoteOrNull(key)).append(':').append(value);
        }
        return sb.append('}').toString();
    }

    // --- the check ------------------------------------------------------------------------------

    public void testOracleMatchesWhatWasWritten() throws IOException {
        byte[] file = writeFile();
        List<Group> rows = oracleRows(file);
        assertEquals("row count", ROWS, rows.size());
        for (int i = 0; i < ROWS; i++) {
            Group row = rows.get(i);
            assertEquals("id at row " + i, i, row.getLong("id", 0));
            assertEquals("items at row " + i, expectedItemsJson(i), groupToJson(row, "items"));
            assertEquals("attrs at row " + i, expectedAttrsJson(i), groupToJson(row, "attrs"));
        }
    }

    // --- step 2: the converter, driven by parquet-mr over parquet-mr's own page store -----------

    /**
     * Reads the named composite columns through {@link JsonRecordMaterializer}, one record reader per
     * column over a shared row group. The columns are disjoint, so their leaf page readers are disjoint
     * too and the readers can be advanced in step; the oracle gets its own pass over the file because it
     * reads every column and a page reader is consumed once.
     *
     * @return per field, one entry per row in file order, null where the column is absent for that row
     */
    private Map<String, List<String>> converterRows(byte[] file, List<String> fields) throws IOException {
        Map<String, List<String>> out = new LinkedHashMap<>();
        for (String field : fields) {
            out.put(field, new ArrayList<>(ROWS));
        }
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(
                    createStorageObject(file),
                    FooterByteCache.fromSettings(Settings.EMPTY),
                    new NoopCircuitBreaker("composite-assembly-test")
                ),
                PlainParquetReadOptions.builder(new PlainCompressionCodecFactory()).build()
            )
        ) {
            MessageType schema = reader.getFileMetaData().getSchema();
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                Map<String, RecordReader<BytesRef>> readers = new LinkedHashMap<>();
                for (String field : fields) {
                    MessageType sub = new MessageType(schema.getName(), schema.getType(field));
                    readers.put(field, new ColumnIOFactory().getColumnIO(sub).getRecordReader(pages, new JsonRecordMaterializer(sub)));
                }
                for (long r = 0; r < pages.getRowCount(); r++) {
                    for (String field : fields) {
                        BytesRef value = readers.get(field).read();
                        out.get(field).add(value == null ? null : value.utf8ToString());
                    }
                }
            }
        }
        return out;
    }

    /**
     * The converter agrees with the oracle and with the independently built expectation, on every row and
     * for all three composite shapes: a list of structs, a map, and a list of primitives.
     * <p>
     * Agreeing with the oracle is the part that matters. Both run over the same leaves at a page size that
     * breaks {@code sku} and {@code price} on different rows, so a converter that paired values by position
     * instead of by repetition level would disagree here.
     */
    public void testConverterMatchesOracleOverParquetMrPages() throws IOException {
        byte[] file = writeFile();
        List<Group> oracle = oracleRows(file);
        assertEquals("oracle row count", ROWS, oracle.size());
        Map<String, List<String>> converted = converterRows(file, List.of("items", "attrs", "tags"));
        for (String field : List.of("items", "attrs", "tags")) {
            assertEquals("converter row count for " + field, ROWS, converted.get(field).size());
        }
        for (int i = 0; i < ROWS; i++) {
            assertEquals("items vs expectation at row " + i, expectedItemsJson(i), converted.get("items").get(i));
            assertEquals("attrs vs expectation at row " + i, expectedAttrsJson(i), converted.get("attrs").get(i));
            assertEquals("tags vs expectation at row " + i, expectedTagsJson(i), converted.get("tags").get(i));
            assertEquals("items vs oracle at row " + i, groupToJson(oracle.get(i), "items"), converted.get("items").get(i));
            assertEquals("attrs vs oracle at row " + i, groupToJson(oracle.get(i), "attrs"), converted.get("attrs").get(i));
        }
    }

    // --- step 3: the same converter over this reader's own page store ---------------------------

    /** One assembled row: its index in the file, and the rendered value of each requested column. */
    private record AssembledRow(long row, Map<String, String> values) {}

    /** Chooses which rows of a row group of the given size are selected. */
    private interface RowGroupRanges {
        RowRanges apply(long rowCount);
    }

    /**
     * Assembles the named composite columns over {@link PrefetchedRowGroupBuilder}'s store rather than
     * parquet-mr's, which is the store the production iterator reads from. Built the way
     * {@code PrefetchedRowGroupBuilderParityTests} builds a row group: preload the metadata, prefetch the
     * selected pages of the projected chunks, then build a store for the row ranges.
     * <p>
     * The row ranges are the point. When a subset is selected, the store exposes the selected row indexes
     * and parquet-mr pairs the leaves through a synchronizing column reader; if a composite's leaves were
     * to lose that, the values would pair up wrongly rather than fail.
     */
    private List<AssembledRow> assembleOverPrefetchedStore(byte[] file, List<String> fields, RowGroupRanges rangesFor) throws IOException {
        List<AssembledRow> out = new ArrayList<>();
        StorageObject storageObject = createStorageObject(file);
        CircuitBreaker breaker = new NoopCircuitBreaker("composite-assembly-test");
        PlainCompressionCodecFactory codecs = new PlainCompressionCodecFactory();
        try (
            ParquetFileReader reader = ParquetFileReader.open(
                new ParquetStorageObjectAdapter(storageObject, FooterByteCache.fromSettings(Settings.EMPTY), breaker),
                PlainParquetReadOptions.builder(codecs).build()
            )
        ) {
            MessageType schema = reader.getFileMetaData().getSchema();
            Set<String> projected = leafPaths(schema, fields);
            List<BlockMetaData> blocks = reader.getRowGroups();
            try (PreloadedRowGroupMetadata metadata = PreloadedRowGroupMetadata.preload(reader, storageObject, breaker)) {
                assertLeavesBreakPagesDifferently(metadata, blocks.size());
                long groupFirstRow = 0;
                for (int ordinal = 0; ordinal < blocks.size(); ordinal++) {
                    BlockMetaData block = blocks.get(ordinal);
                    long rowCount = block.getRowCount();
                    RowRanges ranges = rangesFor.apply(rowCount);
                    ColumnChunkPrefetcher.PrefetchedChunks prefetched = ColumnChunkPrefetcher.fetchSync(
                        storageObject,
                        block,
                        projected,
                        ranges,
                        metadata,
                        ordinal,
                        rowCount,
                        breaker
                    );
                    try (
                        Releasable released = prefetched.release();
                        PageReadStore store = PrefetchedRowGroupBuilder.build(
                            block,
                            ordinal,
                            schema,
                            projected,
                            ranges,
                            metadata,
                            prefetched.chunks(),
                            codecs,
                            breaker
                        )
                    ) {
                        // A selection that is not the whole row group has to reach parquet-mr as selected
                        // row indexes, because that is what makes it build a synchronizing column reader
                        // instead of a plain one. Without this the range cases could pass by reading every
                        // page and never exercising synchronization at all.
                        if (ranges.isAll() == false) {
                            assertTrue("a filtered store must expose its selected row indexes", store.getRowIndexes().isPresent());
                        }
                        Map<String, RecordReader<BytesRef>> readers = new LinkedHashMap<>();
                        for (String field : fields) {
                            MessageType sub = new MessageType(schema.getName(), schema.getType(field));
                            readers.put(
                                field,
                                new ColumnIOFactory().getColumnIO(sub).getRecordReader(store, new JsonRecordMaterializer(sub))
                            );
                        }
                        PrimitiveIterator.OfLong selected = ranges.rowIndexes();
                        while (selected.hasNext()) {
                            long local = selected.nextLong();
                            Map<String, String> values = new LinkedHashMap<>();
                            for (String field : fields) {
                                BytesRef value = readers.get(field).read();
                                values.put(field, value == null ? null : value.utf8ToString());
                            }
                            out.add(new AssembledRow(groupFirstRow + local, values));
                        }
                    }
                    groupFirstRow += rowCount;
                }
            }
        } finally {
            codecs.release();
        }
        return out;
    }

    /** The dotted paths of every leaf under the named top-level fields, which is what the builder projects. */
    private static Set<String> leafPaths(MessageType schema, List<String> fields) {
        Set<String> paths = new LinkedHashSet<>();
        for (ColumnDescriptor descriptor : schema.getColumns()) {
            String dotted = String.join(".", descriptor.getPath());
            for (String field : fields) {
                if (dotted.equals(field) || dotted.startsWith(field + ".")) {
                    paths.add(dotted);
                }
            }
        }
        return paths;
    }

    /**
     * Pins the premise the whole file is built around: {@code sku} and {@code price} really do break pages
     * on different rows in at least one row group. Without that, the range cases below would start and end
     * on a page boundary for both leaves at once and would not exercise synchronization at all.
     */
    private static void assertLeavesBreakPagesDifferently(PreloadedRowGroupMetadata metadata, int rowGroups) {
        for (int ordinal = 0; ordinal < rowGroups; ordinal++) {
            OffsetIndex sku = metadata.getOffsetIndex(ordinal, "items.list.element.sku");
            OffsetIndex price = metadata.getOffsetIndex(ordinal, "items.list.element.price");
            if (sku == null || price == null) {
                continue;
            }
            if (firstRowIndexes(sku).equals(firstRowIndexes(price)) == false) {
                return;
            }
        }
        fail("no row group has sku and price breaking pages on different rows, so synchronization is untested");
    }

    private static List<Long> firstRowIndexes(OffsetIndex index) {
        List<Long> rows = new ArrayList<>(index.getPageCount());
        for (int page = 0; page < index.getPageCount(); page++) {
            rows.add(index.getFirstRowIndex(page));
        }
        return rows;
    }

    /**
     * Three selections over this reader's own store: every row, a window inside each row group, and two
     * windows with a hole between them. All three must assemble exactly what the oracle holds for the rows
     * they select.
     */
    public void testAssemblyOverPrefetchedStore() throws IOException {
        byte[] file = writeFile();
        List<Group> oracle = oracleRows(file);
        List<String> fields = List.of("items", "attrs", "tags");

        List<AssembledRow> everything = assembleOverPrefetchedStore(file, fields, RowRanges::all);
        assertEquals("every row selected", ROWS, everything.size());
        assertAgainstOracle("all rows", everything, oracle);

        List<AssembledRow> window = assembleOverPrefetchedStore(
            file,
            fields,
            rowCount -> RowRanges.of(rowCount / 4, (rowCount * 3) / 4, rowCount)
        );
        assertThat("the window must select something", window.size(), greaterThan(0));
        assertThat("the window must not select everything", window.size(), lessThan(ROWS));
        assertAgainstOracle("mid-page window", window, oracle);

        List<AssembledRow> holed = assembleOverPrefetchedStore(file, fields, rowCount -> {
            List<long[]> ranges = new ArrayList<>();
            ranges.add(new long[] { 0, Math.max(1, rowCount / 4) });
            ranges.add(new long[] { (rowCount * 3) / 4, rowCount });
            return RowRanges.fromUnsorted(ranges, rowCount);
        });
        assertThat("the two intervals must select something", holed.size(), greaterThan(0));
        assertThat("the two intervals must leave a hole", holed.size(), lessThan(ROWS));
        assertAgainstOracle("two intervals", holed, oracle);
    }

    private static void assertAgainstOracle(String what, List<AssembledRow> rows, List<Group> oracle) {
        for (AssembledRow row : rows) {
            int i = (int) row.row();
            assertEquals(what + ": items vs oracle at row " + i, groupToJson(oracle.get(i), "items"), row.values().get("items"));
            assertEquals(what + ": attrs vs oracle at row " + i, groupToJson(oracle.get(i), "attrs"), row.values().get("attrs"));
            assertEquals(what + ": items vs expectation at row " + i, expectedItemsJson(i), row.values().get("items"));
            assertEquals(what + ": attrs vs expectation at row " + i, expectedAttrsJson(i), row.values().get("attrs"));
            assertEquals(what + ": tags vs expectation at row " + i, expectedTagsJson(i), row.values().get("tags"));
        }
    }

    // --- scaffolding ----------------------------------------------------------------------------

    private static OutputFile createOutputFile(ByteArrayOutputStream outputStream) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionStream(outputStream);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return positionStream(outputStream);
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

    private static PositionOutputStream positionStream(ByteArrayOutputStream outputStream) {
        return new PositionOutputStream() {
            @Override
            public long getPos() {
                return outputStream.size();
            }

            @Override
            public void write(int b) {
                outputStream.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) {
                outputStream.write(b, off, len);
            }
        };
    }

    private static StorageObject createStorageObject(byte[] data) {
        return new StorageObject() {
            @Override
            public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

            @Override
            public String version() {
                return "v1";
            }

            @Override
            public StorageIdentity storageIdentity() {
                return AbstractTestStorageObject.NOOP;
            }

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
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://composite-assembly.parquet");
            }
        };
    }
}
