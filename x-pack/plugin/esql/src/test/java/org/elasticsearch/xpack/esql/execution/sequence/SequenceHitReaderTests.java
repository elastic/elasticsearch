/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/** Checks mapped-value fidelity and independent compute block ownership when search responses are released. */
public class SequenceHitReaderTests extends ESTestCase {
    public void testRetainedNativeValuesIncludeDocumentAndMetadataBackingArrays() {
        SearchHit hit = hit();
        BytesRef document = new BytesRef(new byte[1024 * 1024], 1, 17);
        BytesRef metadata = new BytesRef(new byte[64 * 1024], 2, 19);
        hit.addDocumentFields(
            java.util.Map.of("payload", new DocumentField("payload", List.of(document))),
            java.util.Map.of("_ignored", new DocumentField("_ignored", List.of(metadata)))
        );
        assertTrue(SequenceHitReader.retainedBytes(hit) - hit.ramBytesUsed() >= document.bytes.length + metadata.bytes.length);
        SearchHit ordinary = hit();
        ordinary.setDocumentField(new DocumentField("payload", List.of("ordinary text", 7L)));
        assertEquals(ordinary.ramBytesUsed(), SequenceHitReader.retainedBytes(ordinary));
    }

    public void testExactIntegralAndTemporalValues() {
        var factory = factory();
        List<Attribute> schema = List.of(
            field("signed", DataType.LONG),
            field("unsigned", DataType.UNSIGNED_LONG),
            field("millis", DataType.DATETIME),
            field("nanos", DataType.DATE_NANOS)
        );
        var reader = new SequenceHitReader(schema, EsqlTestUtils.TEST_CFG);
        String timestamp = "2026-09-25T21:14:51.389000001Z";
        BigInteger max = new BigInteger("18446744073709551615");
        SearchHit first = hit();
        first.setDocumentField(new DocumentField("signed", List.of("9007199254740993")));
        first.setDocumentField(new DocumentField("unsigned", List.of("0", BigInteger.ONE.shiftLeft(63).toString(), max.toString())));
        first.setDocumentField(new DocumentField("millis", List.of("2026-09-25T21:14:51.389Z")));
        first.setDocumentField(new DocumentField("nanos", List.of(timestamp)));
        try (Page page = reader.read(new SearchHit[] { first }, factory)) {
            assertEquals(9_007_199_254_740_993L, ((LongBlock) page.getBlock(0)).getLong(0));
            LongBlock unsigned = page.getBlock(1);
            assertEquals(3, unsigned.getValueCount(0));
            assertEquals(NumericUtils.asLongUnsigned(BigInteger.ZERO), unsigned.getLong(0));
            assertEquals(NumericUtils.asLongUnsigned(BigInteger.ONE.shiftLeft(63)), unsigned.getLong(1));
            assertEquals(NumericUtils.asLongUnsigned(max), unsigned.getLong(2));
            assertEquals(Instant.parse(timestamp).toEpochMilli(), ((LongBlock) page.getBlock(2)).getLong(0));
            assertEquals(EsqlDataTypeConverter.dateNanosToLong(timestamp), ((LongBlock) page.getBlock(3)).getLong(0));
        }
        factory.ensureAllBlocksAreReleased();
    }

    public void testSourceTimestampRequiresOneNativeDateValue() {
        SearchHit valid = hit();
        for (long timestamp : List.of(0L, -1L, 9_007_199_254_740_993L)) {
            valid.setDocumentField(new DocumentField("@timestamp", List.of(timestamp)));
            SequenceHitReader.ensureScalarTimestamp(valid);
        }
        valid.setDocumentField(new DocumentField("@timestamp", List.of("2026-09-25T21:14:51.389000001Z")));
        SequenceHitReader.ensureScalarTimestamp(valid);
        for (List<?> values : List.of(List.of(), List.of(1L, 2L), List.of("not-an-instant"))) {
            SearchHit invalid = hit();
            invalid.setDocumentField(new DocumentField("@timestamp", new ArrayList<>(values)));
            assertEquals(
                "SEQUENCE source [@timestamp] must hold a single mapped date or date_nanos value",
                expectThrows(IllegalArgumentException.class, () -> SequenceHitReader.ensureScalarTimestamp(invalid)).getMessage()
            );
        }
        expectThrows(IllegalArgumentException.class, () -> SequenceHitReader.ensureScalarTimestamp(hit()));
    }

    public void testTimestampIsFetchedAfterVisiblePayloadPruningAndDeduplicatedWhenRetained() {
        for (List<Attribute> attributes : List.of(List.<Attribute>of(), List.of(metadata("_id", DataType.KEYWORD)))) {
            var request = new SequenceHitReader(attributes, EsqlTestUtils.TEST_CFG).fields(new SearchSourceBuilder());
            var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
            assertEquals(List.of("@timestamp"), projection.fields().stream().map(field -> field.field).toList());
            assertNull(projection.fields().getFirst().format);
            assertFalse(request.fetchSource().fetchSource());
        }
        var request = new SequenceHitReader(List.of(field("@timestamp", DataType.DATE_NANOS)), EsqlTestUtils.TEST_CFG).fields(
            new SearchSourceBuilder()
        );
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(List.of("@timestamp"), projection.fields().stream().map(field -> field.field).toList());
        assertEquals(SequenceHitReader.TEMPORAL_FORMAT, projection.fields().getFirst().format);
    }

    /** Identity fetches evaluate only demanded mapped values, including when the replay has no mapped fields. */
    public void testIdentityProjectionDoesNotDemandTimestamp() {
        for (List<Attribute> attributes : List.of(
            List.<Attribute>of(),
            List.of(metadata("_id", DataType.KEYWORD), metadata("_index", DataType.KEYWORD)),
            List.of(metadata("_source", DataType.SOURCE))
        )) {
            var request = new SequenceHitReader(attributes, EsqlTestUtils.TEST_CFG).identityFields(new SearchSourceBuilder());
            assertTrue(request.ext().isEmpty());
            assertEquals(attributes.stream().anyMatch(a -> a.dataType() == DataType.SOURCE), request.fetchSource().fetchSource());
            assertNull(request.docValueFields());
            assertNull(request.fetchFields());
        }
        var request = new SequenceHitReader(List.of(field("payload", DataType.LONG)), EsqlTestUtils.TEST_CFG).identityFields(
            new SearchSourceBuilder()
        );
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(List.of("payload"), projection.fields().stream().map(field -> field.field).toList());
    }

    /** A genuinely demanded live timestamp is an ordinary mapped value; scalar ordering was checked at admission. */
    public void testIdentityTimestampKeepsDemandedOrdinaryValues() {
        var factory = factory();
        var reader = new SequenceHitReader(List.of(field("@timestamp", DataType.DATE_NANOS)), EsqlTestUtils.TEST_CFG);
        var request = reader.identityFields(new SearchSourceBuilder());
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(List.of("@timestamp"), projection.fields().stream().map(field -> field.field).toList());
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("@timestamp", List.of("1970-01-01T00:00:00.000000001Z", "1970-01-01T00:00:00.000000002Z")));
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            LongBlock values = page.getBlock(0);
            assertEquals(2, values.getValueCount(0));
            assertEquals(1L, values.getLong(values.getFirstValueIndex(0)));
            assertEquals(2L, values.getLong(values.getFirstValueIndex(0) + 1));
        }
        factory.ensureAllBlocksAreReleased();
    }

    public void testTemporalProjectionRequestsIsoAndRejectsAmbiguousNativeLong() {
        var reader = new SequenceHitReader(
            List.of(field("millis", DataType.DATETIME), field("nanos", DataType.DATE_NANOS), field("payload", DataType.LONG)),
            EsqlTestUtils.TEST_CFG
        );
        for (SearchSourceBuilder request : List.of(
            reader.fields(new SearchSourceBuilder()),
            reader.identityFields(new SearchSourceBuilder())
        )) {
            var fields = ((SequenceDocValuesSearchExt) request.ext().getFirst()).fields();
            assertEquals(
                SequenceHitReader.TEMPORAL_FORMAT,
                fields.stream().filter(field -> field.field.equals("millis")).findFirst().orElseThrow().format
            );
            assertEquals(
                SequenceHitReader.TEMPORAL_FORMAT,
                fields.stream().filter(field -> field.field.equals("nanos")).findFirst().orElseThrow().format
            );
            assertEquals(
                SequenceHitReader.INTEGRAL_FORMAT,
                fields.stream().filter(field -> field.field.equals("payload")).findFirst().orElseThrow().format
            );
        }
        var factory = factory();
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("millis", List.of(1_773_750_896_789L)));
        assertEquals(
            "SEQUENCE mapped temporal values require a formatted date instant",
            expectThrows(IllegalArgumentException.class, () -> reader.read(new SearchHit[] { hit }, factory)).getMessage()
        );
        factory.ensureAllBlocksAreReleased();
    }

    public void testFormattedTemporalInstantsConvertIntoFrozenMillisAndNanos() {
        var factory = factory();
        var reader = new SequenceHitReader(
            List.of(field("nanos", DataType.DATE_NANOS), field("millis", DataType.DATETIME), field("missing", DataType.DATE_NANOS)),
            EsqlTestUtils.TEST_CFG
        );
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("nanos", List.of("2026-03-17T12:34:56.789Z")));
        hit.setDocumentField(new DocumentField("millis", List.of("2026-03-17T12:34:56.789000000Z", "2026-03-17T12:34:56.790000000Z")));
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            assertEquals(1_773_750_896_789_000_000L, ((LongBlock) page.getBlock(0)).getLong(0));
            LongBlock millis = page.getBlock(1);
            assertEquals(2, millis.getValueCount(0));
            assertEquals(1_773_750_896_789L, millis.getLong(0));
            assertEquals(1_773_750_896_790L, millis.getLong(1));
            assertTrue(page.getBlock(2).isNull(0));
        }
        hit.setDocumentField(new DocumentField("millis", List.of("2026-03-17T12:34:56.789123456Z")));
        assertEquals(
            "SEQUENCE temporal instant cannot be represented with date millisecond precision",
            expectThrows(IllegalArgumentException.class, () -> reader.read(new SearchHit[] { hit }, factory)).getMessage()
        );
        factory.ensureAllBlocksAreReleased();
    }

    public void testMetadataAndSourceAreRetainedFromSelectedHit() {
        var factory = factory();
        var reader = new SequenceHitReader(
            List.of(
                metadata("_id", DataType.KEYWORD),
                metadata("_index", DataType.KEYWORD),
                metadata("_version", DataType.LONG),
                metadata("_source", DataType.SOURCE),
                metadata("_score", DataType.DOUBLE)
            ),
            EsqlTestUtils.TEST_CFG
        );
        SearchHit hit = hit();
        hit.version(17);
        hit.score(1.25f);
        hit.sourceRef(new BytesArray("{\"key\":\"original\",\"nested\":{\"value\":1}}"));
        var request = reader.identityFields(new SearchSourceBuilder());
        assertTrue(request.ext().isEmpty());
        assertEquals(Boolean.TRUE, request.version());
        assertTrue(request.trackScores());
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            assertEquals("document", text(page, 0));
            assertEquals("events", text(page, 1));
            assertEquals(17, ((LongBlock) page.getBlock(2)).getLong(0));
            assertEquals("{\"key\":\"original\",\"nested\":{\"value\":1}}", text(page, 3));
            assertEquals(1.25, ((DoubleBlock) page.getBlock(4)).getDouble(0), 0.0);
        }
        factory.ensureAllBlocksAreReleased();
    }

    public void testMissingValuesKeepTheirTypedSchema() {
        var factory = factory();
        List<Attribute> attributes = List.of(
            field("keyword", DataType.KEYWORD),
            field("long", DataType.LONG),
            field("integer", DataType.INTEGER),
            field("double", DataType.DOUBLE),
            field("boolean", DataType.BOOLEAN)
        );
        try (Page page = new SequenceHitReader(attributes, EsqlTestUtils.TEST_CFG).read(new SearchHit[] { hit(), hit() }, factory)) {
            assertEquals(2, page.getPositionCount());
            assertEquals(
                List.of(ElementType.BYTES_REF, ElementType.LONG, ElementType.INT, ElementType.DOUBLE, ElementType.BOOLEAN),
                java.util.stream.IntStream.range(0, page.getBlockCount()).mapToObj(i -> page.getBlock(i).elementType()).toList()
            );
            for (int channel = 0; channel < page.getBlockCount(); channel++) {
                assertTrue(page.getBlock(channel).isNull(0));
                assertTrue(page.getBlock(channel).isNull(1));
            }
        }
        factory.ensureAllBlocksAreReleased();
    }

    public void testFailedConversionReleasesAlreadyBuiltColumns() {
        var factory = factory();
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("keyword", List.of(new BytesRef("valid"))));
        hit.setDocumentField(new DocumentField("long", List.of("invalid")));
        var reader = new SequenceHitReader(
            List.of(field("keyword", DataType.KEYWORD), field("long", DataType.LONG)),
            EsqlTestUtils.TEST_CFG
        );
        expectThrows(RuntimeException.class, () -> reader.read(new SearchHit[] { hit }, factory));
        factory.ensureAllBlocksAreReleased();
    }

    public void testMappedFieldsUseInternalBlockLoadersAndMetadataRequestsSource() {
        var reader = new SequenceHitReader(
            List.of(
                field("keyword", DataType.KEYWORD),
                field("nanos", DataType.DATE_NANOS),
                metadata("_id", DataType.KEYWORD),
                metadata("_version", DataType.LONG),
                metadata("_source", DataType.SOURCE)
            ),
            EsqlTestUtils.TEST_CFG
        );
        SearchSourceBuilder request = reader.fields(new SearchSourceBuilder());
        assertNull(request.docValueFields());
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(List.of("@timestamp", "keyword", "nanos"), projection.fields().stream().map(f -> f.field).toList());
        assertEquals(SequenceHitReader.TEMPORAL_FORMAT, projection.fields().get(2).format);
        assertTrue(request.fetchSource().fetchSource());
        assertEquals(Boolean.TRUE, request.version());
        assertNull(request.fetchFields());
    }

    public void testWideMappedProjectionDoesNotUsePublicDocValueFields() {
        List<Attribute> attributes = new ArrayList<>();
        for (int column = 0; column < 256; column++) {
            attributes.add(field("value" + column, DataType.LONG));
        }
        attributes.add(field("value0", DataType.LONG));
        var request = new SequenceHitReader(attributes, EsqlTestUtils.TEST_CFG).fields(new SearchSourceBuilder());
        assertNull(request.docValueFields());
        assertNull(request.fetchFields());
        assertFalse(request.fetchSource().fetchSource());
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(257, projection.fields().size());
        assertEquals("@timestamp", projection.fields().getFirst().field);
        assertEquals("value0", projection.fields().get(1).field);
        assertEquals("value255", projection.fields().getLast().field);
    }

    public void testTextAndNonAggregatableFieldsUseMappedBlockLoaders() {
        var stored = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "stored",
            new EsField("stored", DataType.KEYWORD, java.util.Map.of(), false, NONE),
            false
        );
        var request = new SequenceHitReader(List.of(field("text", DataType.TEXT), stored), EsqlTestUtils.TEST_CFG).fields(
            new SearchSourceBuilder()
        );
        assertNull(request.fetchFields());
        assertNull(request.docValueFields());
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(List.of("@timestamp", "text", "stored"), projection.fields().stream().map(f -> f.field).toList());
    }

    public void testSemanticIpAndVersionAndNativeUtf8AreCopiedIntoOwnedBlocks() {
        var factory = factory();
        var reader = new SequenceHitReader(
            List.of(field("ip", DataType.IP), field("version", DataType.VERSION), field("flattened", DataType.FLATTENED)),
            EsqlTestUtils.TEST_CFG
        );
        BytesRef ip = (BytesRef) EsqlDataTypeConverter.convert("2001:db8::1", DataType.IP, EsqlTestUtils.TEST_CFG);
        BytesRef version = (BytesRef) EsqlDataTypeConverter.convert("1.2.3-beta", DataType.VERSION, EsqlTestUtils.TEST_CFG);
        BytesRef flattened = new BytesRef("{\"nested.key\":\"value\"}");
        List<BytesRef> encoded = List.of(ip, version, flattened);
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("ip", List.of("2001:db8::1")));
        hit.setDocumentField(new DocumentField("version", List.of("1.2.3-beta")));
        hit.setDocumentField(new DocumentField("flattened", List.of(flattened)));
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            for (int field = 0; field < encoded.size(); field++) {
                BytesRef expected = BytesRef.deepCopyOf(encoded.get(field));
                encoded.get(field).bytes[encoded.get(field).offset] ^= 1;
                assertEquals(expected, ((BytesRefBlock) page.getBlock(field)).getBytesRef(0, new BytesRef()));
            }
        }
        factory.ensureAllBlocksAreReleased();
    }

    public void testIntegralProjectionAndExactMappedBounds() {
        List<DataType> types = List.of(
            DataType.INTEGER,
            DataType.COUNTER_INTEGER,
            DataType.LONG,
            DataType.COUNTER_LONG,
            DataType.UNSIGNED_LONG
        );
        var request = new SequenceHitReader(types.stream().map(type -> field(type.typeName(), type)).toList(), EsqlTestUtils.TEST_CFG)
            .identityFields(new SearchSourceBuilder());
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(types.size(), projection.fields().size());
        assertTrue(projection.fields().stream().allMatch(field -> SequenceHitReader.INTEGRAL_FORMAT.equals(field.format)));
        for (DataType type : List.of(DataType.INTEGER, DataType.COUNTER_INTEGER)) {
            assertMapped(type, List.of("-2147483648", "0", "2147483647"), List.of(Integer.MIN_VALUE, 0, Integer.MAX_VALUE));
            assertMappedFailure(type, "2147483648");
        }
        for (DataType type : List.of(DataType.LONG, DataType.COUNTER_LONG)) {
            assertMapped(
                type,
                List.of("-9223372036854775808", "9007199254740993", "9223372036854775807"),
                List.of(Long.MIN_VALUE, 9_007_199_254_740_993L, Long.MAX_VALUE)
            );
            assertMappedFailure(type, "18446744073709551615");
        }
        assertMapped(
            DataType.UNSIGNED_LONG,
            List.of("0", "1", "9223372036854775808", "18446744073709551615"),
            List.of(
                NumericUtils.asLongUnsigned(BigInteger.ZERO),
                NumericUtils.asLongUnsigned(BigInteger.ONE),
                NumericUtils.asLongUnsigned(BigInteger.ONE.shiftLeft(63)),
                NumericUtils.asLongUnsigned(NumericUtils.UNSIGNED_LONG_MAX)
            )
        );
        assertMappedFailure(DataType.UNSIGNED_LONG, "-1");
        assertMappedFailure(DataType.UNSIGNED_LONG, "18446744073709551616");
        for (DataType type : types) {
            assertMappedFailure(type, "1.5");
            assertMappedFailure(type, 1L);
            assertMappedFailure(type, new BytesRef("1"));
        }
    }

    public void testMappedDoubleRequiresExactIntegralConversionAndPreservesNativeFloatingValues() {
        for (DataType type : List.of(DataType.DOUBLE, DataType.COUNTER_DOUBLE)) {
            for (double value : new double[] { -0.0, 1.25, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY }) {
                assertMapped(type, List.of(value), value);
            }
            assertMapped(type, List.of(1.25f), 1.25);
            assertMapped(type, List.of(9_007_199_254_740_992L), 9_007_199_254_740_992.0);
            assertMapped(type, List.of("9223372036854775808"), 0x1.0p63);
            assertMapped(type, List.of(Long.MIN_VALUE), -0x1.0p63);
            assertMappedFailure(type, 9_007_199_254_740_993L);
            assertMappedFailure(type, "18446744073709551615");
            assertMappedFailure(type, new BytesRef("1"));
            assertMappedFailure(type, true);
        }
    }

    /** Frozen textual consumers must not stringify values supplied by an incompatible live field family. */
    public void testMappedTextRequiresNativeBytesAndPreservesMissingAndMultivaluedValues() {
        for (DataType type : List.of(DataType.KEYWORD, DataType.TEXT, DataType.FLATTENED)) {
            assertMapped(type, List.of(new BytesRef("a"), new BytesRef("b")), List.of(new BytesRef("a"), new BytesRef("b")));
            assertMapped(type, List.of(new BytesRef("")), new BytesRef(""));
            assertMapped(type, java.util.Collections.singletonList(null), null);
            for (Object value : List.of("semantic text", 7, 7L, 1.5, true)) {
                var factory = factory();
                SearchHit hit = hit();
                hit.setDocumentField(new DocumentField("value", List.of(value)));
                var reader = new SequenceHitReader(List.of(field("value", type)), EsqlTestUtils.TEST_CFG);
                assertThat(
                    expectThrows(IllegalArgumentException.class, () -> reader.read(new SearchHit[] { hit }, factory)).getMessage(),
                    containsString("SEQUENCE mapped [" + type.typeName() + "] values require native textual bytes")
                );
                factory.ensureAllBlocksAreReleased();
            }
        }
    }

    public void testOnlySemanticIpVersionAndActualBooleanEnterFrozenMappedTypes() {
        for (String address : List.of("192.0.2.1", "2001:db8::1")) {
            assertMapped(DataType.IP, List.of(address), EsqlDataTypeConverter.convert(address, DataType.IP, EsqlTestUtils.TEST_CFG));
        }
        // Semantic strings do not retain their producer family; VERSION intentionally also accepts invalid-text versions.
        for (String version : List.of("1.2.3", "1.2.3-beta", "not a version", "7", "")) {
            assertMapped(
                DataType.VERSION,
                List.of(version),
                EsqlDataTypeConverter.convert(version, DataType.VERSION, EsqlTestUtils.TEST_CFG)
            );
        }
        assertMappedFailure(DataType.IP, new BytesRef("abcdefghijklmnop"));
        assertMappedFailure(DataType.VERSION, new BytesRef("1.2.3"));
        assertMapped(DataType.BOOLEAN, List.of(true, false), List.of(true, false));
        assertMappedFailure(DataType.BOOLEAN, "true");
        assertMappedFailure(DataType.BOOLEAN, 1L);
    }

    /** Point blocks retain the mapper's WKB, precision, duplicates, and list order without textual decoding. */
    public void testNativePointCellsKeepWkbAndRequestTheirFrozenFamily() {
        BytesRef first = new BytesRef(WellKnownBinary.toWKB(new Point(12.123456789123, -4.987654321987), ByteOrder.LITTLE_ENDIAN));
        BytesRef second = new BytesRef(WellKnownBinary.toWKB(new Point(-3.987654321987, 8.123456789123), ByteOrder.LITTLE_ENDIAN));
        for (DataType type : List.of(DataType.GEO_POINT, DataType.CARTESIAN_POINT)) {
            assertMapped(type, List.of(first), first);
            assertMapped(type, List.of(second, first, second), List.of(second, first, second));
            assertMapped(type, List.of(), null);
            assertMappedFailure(type, 1L);
            assertMappedFailure(type, "POINT (1 2)");
            var reader = new SequenceHitReader(List.of(field("value", type)), EsqlTestUtils.TEST_CFG);
            for (SearchSourceBuilder request : List.of(
                reader.fields(new SearchSourceBuilder()),
                reader.identityFields(new SearchSourceBuilder())
            )) {
                var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
                assertEquals(
                    type == DataType.GEO_POINT ? SequenceHitReader.GEO_POINT_FORMAT : SequenceHitReader.CARTESIAN_POINT_FORMAT,
                    projection.fields().stream().filter(field -> field.field.equals("value")).findFirst().orElseThrow().format
                );
            }
        }
    }

    private static void assertMapped(DataType type, List<Object> values, Object expected) {
        var factory = factory();
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("value", values));
        var reader = new SequenceHitReader(List.of(field("value", type), field("missing", type)), EsqlTestUtils.TEST_CFG);
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            assertEquals(expected, BlockUtils.toJavaObject(page.getBlock(0), 0));
            assertTrue(page.getBlock(1).isNull(0));
        }
        factory.ensureAllBlocksAreReleased();
    }

    private static void assertMappedFailure(DataType type, Object value) {
        var factory = factory();
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("value", List.of(value)));
        var reader = new SequenceHitReader(List.of(field("value", type)), EsqlTestUtils.TEST_CFG);
        expectThrows(IllegalArgumentException.class, () -> reader.read(new SearchHit[] { hit }, factory));
        factory.ensureAllBlocksAreReleased();
    }

    public void testUnsupportedFieldsAreNullAndNotRequested() {
        var factory = factory();
        var reader = new SequenceHitReader(List.of(field("unsupported", DataType.UNSUPPORTED)), EsqlTestUtils.TEST_CFG);
        SearchHit hit = hit();
        hit.setDocumentField(new DocumentField("unsupported", List.of("must remain null")));
        var request = reader.fields(new SearchSourceBuilder());
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(List.of("@timestamp"), projection.fields().stream().map(field -> field.field).toList());
        assertNull(request.fetchFields());
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            assertEquals(ElementType.BYTES_REF, page.getBlock(0).elementType());
            assertTrue(page.getBlock(0).isNull(0));
        }
        factory.ensureAllBlocksAreReleased();
    }

    private static Attribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, null, null, name, new EsField(name, type, java.util.Map.of(), true, NONE), false);
    }

    private static Attribute metadata(String name, DataType type) {
        return new MetadataAttribute(Source.EMPTY, name, type, false);
    }

    private static SearchHit hit() {
        var hit = SearchHit.unpooled(0, "document");
        hit.shard(new SearchShardTarget("node", new ShardId("events", "uuid", 0), null));
        return hit;
    }

    private static String text(Page page, int channel) {
        return ((BytesRefBlock) page.getBlock(channel)).getBytesRef(0, new BytesRef()).utf8ToString();
    }

    private static MockBlockFactory factory() {
        return new MockBlockFactory(
            BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("sequence-reader"))
        );
    }
}
