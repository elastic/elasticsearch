/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
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
import java.util.List;

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
                .withRowGroupSize(8 * 1024L)
                .withPageSize(512)
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
