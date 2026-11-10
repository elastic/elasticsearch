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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.MockBlockFactory;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;

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
        first.setDocumentField(new DocumentField("signed", List.of(9_007_199_254_740_993L)));
        first.setDocumentField(
            new DocumentField(
                "unsigned",
                List.of(
                    NumericUtils.asLongUnsigned(BigInteger.ZERO),
                    NumericUtils.asLongUnsigned(BigInteger.ONE.shiftLeft(63)),
                    NumericUtils.asLongUnsigned(max)
                )
            )
        );
        first.setDocumentField(new DocumentField("millis", List.of(Instant.parse(timestamp).toEpochMilli())));
        first.setDocumentField(new DocumentField("nanos", List.of(EsqlDataTypeConverter.dateNanosToLong(timestamp))));
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
        for (List<?> values : List.of(List.of(), List.of(1L, 2L), List.of("2026-09-25T21:14:51Z"))) {
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
            assertFalse(request.fetchSource().fetchSource());
        }
        var request = new SequenceHitReader(List.of(field("@timestamp", DataType.DATE_NANOS)), EsqlTestUtils.TEST_CFG).fields(
            new SearchSourceBuilder()
        );
        var projection = (SequenceDocValuesSearchExt) request.ext().getFirst();
        assertEquals(List.of("@timestamp"), projection.fields().stream().map(field -> field.field).toList());
    }

    public void testMetadataAndSourceAreRetainedFromSelectedHit() {
        var factory = factory();
        var reader = new SequenceHitReader(
            List.of(
                metadata("_id", DataType.KEYWORD),
                metadata("_index", DataType.KEYWORD),
                metadata("_version", DataType.LONG),
                metadata("_source", DataType.SOURCE)
            ),
            EsqlTestUtils.TEST_CFG
        );
        SearchHit hit = hit();
        hit.version(17);
        hit.sourceRef(new BytesArray("{\"key\":\"original\",\"nested\":{\"value\":1}}"));
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            assertEquals("document", text(page, 0));
            assertEquals("events", text(page, 1));
            assertEquals(17, ((LongBlock) page.getBlock(2)).getLong(0));
            assertEquals("{\"key\":\"original\",\"nested\":{\"value\":1}}", text(page, 3));
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
        hit.setDocumentField(new DocumentField("keyword", List.of("valid")));
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
        assertNull(projection.fields().get(2).format);
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

    public void testNativeEncodedBytesRemainExactAndAreCopiedIntoOwnedBlocks() {
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
        for (int field = 0; field < encoded.size(); field++) {
            hit.setDocumentField(new DocumentField(List.of("ip", "version", "flattened").get(field), List.of(encoded.get(field))));
        }
        try (Page page = reader.read(new SearchHit[] { hit }, factory)) {
            for (int field = 0; field < encoded.size(); field++) {
                BytesRef expected = BytesRef.deepCopyOf(encoded.get(field));
                encoded.get(field).bytes[encoded.get(field).offset] ^= 1;
                assertEquals(expected, ((BytesRefBlock) page.getBlock(field)).getBytesRef(0, new BytesRef()));
            }
        }
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
