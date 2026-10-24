/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.lucene.read.DelegatingBlockLoaderFactory;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.xpack.esql.core.util.TimeSeriesMetadata;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

/** Reads real indexed dimensions so residual labels agree with promoted columns. */
public class TimeSeriesMetadataBlockLoaderTests extends MapperServiceTestCase {
    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new UnsignedLongMapperPlugin());
    }

    public void testUnsignedLongIdentityIsExact() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("sequence").field("type", "unsigned_long").field("time_series_dimension", true).endObject();
        }));
        for (String value : List.of("9007199254740993", "9223372036854775808", "18446744073709551615")) {
            assertEquals("{\"sequence\":\"" + value + "\"}", readMetadata(mapper, Set.of(), "{\"sequence\":" + value + "}"));
        }
    }

    public void testRepeatedDocumentsAndOffset() throws Exception {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        var first = mapper.documentMapper().parse(source("{\"job\":\"first\"}"));
        var second = mapper.documentMapper().parse(source("{\"job\":\"second\"}"));
        withLuceneIndex(mapper, writer -> {
            writer.addDocument(first.rootDoc());
            writer.addDocument(second.rootDoc());
            writer.forceMerge(1);
        }, reader -> {
            var loader = TimeSeriesMetadataBlockLoader.create(createSearchExecutionContext(mapper, new IndexSearcher(reader)), Set.of());
            var blocks = TestBlockFactory.getNonBreakingInstance();
            var factory = loaderFactory(blocks);
            try (var values = loader.columnAtATimeReader(reader.leaves().getFirst()).apply(blocks.breaker())) {
                assertTrue(values.canReuse(0));
                try (var output = (BytesRefBlock) values.read(factory, docs(0, 0, 1, 1), 1, false)) {
                    assertEquals(3, output.getPositionCount());
                    for (int p = 0; p < output.getPositionCount(); p++) {
                        var metadata = TimeSeriesMetadata.read(output.getBytesRef(output.getFirstValueIndex(p), new BytesRef()));
                        assertEquals(List.of(p == 0 ? "first" : "second"), metadata.get("job"));
                    }
                }
                assertFalse(values.canReuse(0));
                assertTrue(values.canReuse(1));
                try (var output = (BytesRefBlock) values.read(factory, docs(1), 0, false)) {
                    assertEquals("{\"job\":\"second\"}", metadata(output));
                }
            }
        });
    }

    public void testKeywordNumericTokensKeepIndexedSpelling() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        for (String token : List.of("1e3", "1000.0", "1.00", "9007199254740993.0")) {
            String json = "{\"job\":" + token + "}";
            var parsed = mapper.documentMapper().parse(source(json));
            List<String> indexed = parsed.rootDoc()
                .getFields("job")
                .stream()
                .filter(field -> field.fieldType().indexOptions() != IndexOptions.NONE)
                .map(field -> field.binaryValue().utf8ToString())
                .toList();
            assertEquals(List.of(token), indexed);
            assertEquals(List.of(token), TimeSeriesMetadata.read(new BytesRef(readMetadata(mapper, Set.of(), json))).get("job"));
        }
    }

    public void testFlattenedNumericTokensKeepIndexedSpelling() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("tags").field("type", "flattened").array("time_series_dimensions", "job").endObject();
        }));
        for (String token : List.of("1e3", "1000.0", "1.00", "9007199254740993.0")) {
            String json = "{\"tags\":{\"job\":" + token + "}}";
            var parsed = mapper.documentMapper().parse(source(json));
            List<String> indexed = parsed.rootDoc()
                .getFields("tags._keyed")
                .stream()
                .filter(field -> field.fieldType().indexOptions() != IndexOptions.NONE)
                .map(field -> field.binaryValue().utf8ToString())
                .toList();
            assertEquals(List.of("job\u0000" + token), indexed);
            assertEquals(List.of(token), TimeSeriesMetadata.read(new BytesRef(readMetadata(mapper, Set.of(), json))).get("tags.job"));
        }
    }

    public void testCircuitBreakerFailureReleasesReadersAndPartialOutput() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        var arrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofKb(32)).withCircuitBreaking();
        var blocks = BlockFactory.builder(arrays).build();
        var breaker = arrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var parsed = mapper.documentMapper().parse(source("{\"job\":\"" + "x".repeat(12000) + "\"}"));
        withLuceneIndex(mapper, writer -> writer.addDocument(parsed.rootDoc()), reader -> {
            var loader = TimeSeriesMetadataBlockLoader.create(createSearchExecutionContext(mapper, new IndexSearcher(reader)), Set.of());
            var factory = loaderFactory(blocks);
            try (var values = loader.columnAtATimeReader(reader.leaves().getFirst()).apply(breaker)) {
                expectThrows(CircuitBreakingException.class, () -> {
                    try (var unexpected = values.read(factory, docs(0), 0, false)) {
                        // Close a successful result before reporting the missing breaker trip.
                    }
                });
            } finally {
                assertEquals(0L, breaker.getUsed());
            }
        });
    }

    public void testMappedValuesAndNames() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("labels").startObject("properties");
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("empty").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
            b.startObject("sequence").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("enabled").field("type", "boolean").field("time_series_dimension", true).endObject();
            b.startObject("address").field("type", "ip").field("time_series_dimension", true).endObject();
            b.startObject("attributes").startObject("properties");
            b.startObject("cpu").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
        String input = """
            {"labels":{"empty":"","job":"api"},"sequence":"001","enabled":"false",
             "address":"2001:0db8:0000:0000:0000:0000:0000:0001","attributes":{"cpu":"0"}}
            """;
        assertEquals(
            "{\"address\":\"2001:db8::1\",\"attributes.cpu\":\"0\",\"enabled\":\"false\",\"job\":\"api\",\"sequence\":\"1\"}",
            readMetadata(mapper, Set.of(), input)
        );
        assertEquals(
            "{\"address\":\"2001:db8::1\",\"attributes.cpu\":\"0\",\"enabled\":\"false\",\"job\":\"api\",\"sequence\":\"1\"}",
            readMetadata(mapper, Set.of(), input.replace("\"001\"", "1").replace("\"false\"", "false"))
        );
    }

    public void testCopiedAndMultifieldDimensions() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("@timestamp").field("type", "date").endObject();
            b.startObject("host").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("input").field("type", "keyword").field("copy_to", "copied").endObject();
            b.startObject("copied").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("message").field("type", "keyword").startObject("fields");
            b.startObject("dimension").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
        String json = "{\"@timestamp\":\"2024-01-01T00:01:00Z\",\"host\":\"a\",\"input\":\"copied-value\",\"message\":\"parent-value\"}";
        var parsed = mapper.documentMapper().parse(source(json));
        assertNotNull(parsed.rootDoc().getField("copied"));
        assertNotNull(parsed.rootDoc().getField("message.dimension"));
        String expected = "{\"copied\":\"copied-value\",\"host\":\"a\",\"message.dimension\":\"parent-value\"}";
        assertEquals(expected, readMetadata(mapper, Set.of(), json));
    }

    public void testExclusionsResolveAliases() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("priority", 1).startObject("properties");
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("instance").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
        String input = "{\"labels\":{\"job\":\"api\",\"instance\":\"a\"}}";
        assertEquals("{\"instance\":\"a\"}", readMetadata(mapper, Set.of("job"), input));
        assertEquals(readMetadata(mapper, Set.of("job"), input), readMetadata(mapper, Set.of("labels.job"), input));
    }

    public void testFlattenedDimensionsExcludeOtherKeys() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("tags").field("type", "flattened").array("time_series_dimensions", "job", "zone").endObject();
        }));
        String input = "{\"tags\":{\"job\":\"api\",\"zone\":\"west\",\"message\":\"ignored\"}}";
        assertEquals("{\"tags.job\":\"api\",\"tags.zone\":\"west\"}", readMetadata(mapper, Set.of(), input));
        assertEquals("{\"tags.zone\":\"west\"}", readMetadata(mapper, Set.of("tags.job"), input));
    }

    public void testAllDimensionsPromotedUseConstantMetadata() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("labels").field("type", "passthrough").field("priority", 1).startObject("properties");
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
            b.startObject("tags").field("type", "flattened").array("time_series_dimensions", "zone").endObject();
        }));
        var parsed = mapper.documentMapper().parse(source("{\"labels\":{\"job\":\"api\"},\"tags\":{\"zone\":\"west\"}}"));
        withLuceneIndex(mapper, writer -> writer.addDocument(parsed.rootDoc()), reader -> {
            var loader = TimeSeriesMetadataBlockLoader.create(
                createSearchExecutionContext(mapper, new IndexSearcher(reader)),
                Set.of("job", "tags.zone")
            );
            var blocks = TestBlockFactory.getNonBreakingInstance();
            try (
                var values = loader.columnAtATimeReader(reader.leaves().getFirst()).apply(blocks.breaker());
                var output = (BytesRefBlock) values.read(loaderFactory(blocks), docs(0, 0, 0), 1, false)
            ) {
                assertEquals(2, output.getPositionCount());
                assertNotNull(output.asVector());
                assertTrue(output.asVector().isConstant());
                assertEquals("{}", output.getBytesRef(0, new BytesRef()).utf8ToString());
            }
        });
    }

    /** Flattened dimensions apply their mapping's null replacement only to explicit source nulls. */
    public void testFlattenedNullValue() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("tags")
                .field("type", "flattened")
                .array("time_series_dimensions", "job")
                .field("null_value", "unknown")
                .endObject();
        }));
        assertEquals("{\"tags.job\":\"unknown\"}", readMetadata(mapper, Set.of(), "{\"tags\":{\"job\":null}}"));
        assertEquals("{}", readMetadata(mapper, Set.of(), "{\"tags\":{}}"));
        assertEquals("{}", readMetadata(mapper, Set.of("tags.job"), "{\"tags\":{\"job\":null}}"));
    }

    /** An object is indexed under descendant keys; it is not a label value for its own flattened key. */
    public void testFlattenedObjectIsNotALeafValue() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("tags").field("type", "flattened").field("index", true).array("time_series_dimensions", "job").endObject();
        }));
        String object = "{\"tags\":{\"job\":{\"sub\":\"x\"}}}";
        var parsed = mapper.documentMapper().parse(source(object));
        var indexed = parsed.rootDoc()
            .getFields("tags._keyed")
            .stream()
            .filter(field -> field.fieldType().indexOptions() != IndexOptions.NONE)
            .map(field -> field.binaryValue().utf8ToString())
            .toList();
        assertThat(indexed, containsInAnyOrder("job.sub\u0000x"));
        assertEquals("{}", readMetadata(mapper, Set.of(), object));

        String mixed = "{\"tags\":{\"job\":[\"api\",{\"sub\":\"x\"}]}}";
        parsed = mapper.documentMapper().parse(source(mixed));
        indexed = parsed.rootDoc()
            .getFields("tags._keyed")
            .stream()
            .filter(field -> field.fieldType().indexOptions() != IndexOptions.NONE)
            .map(field -> field.binaryValue().utf8ToString())
            .toList();
        assertThat(indexed, containsInAnyOrder("job\u0000api", "job.sub\u0000x"));
        assertEquals("{\"tags.job\":\"api\"}", readMetadata(mapper, Set.of(), mixed));
    }

    public void testEmptyAndNullValues() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        for (String input : new String[] { null, "{}", "{\"job\":null}", "{\"job\":\"\"}" }) {
            assertEquals("{}", readMetadata(mapper, Set.of(), input));
        }
    }

    public void testMultipleValuesRetainDimensionIdentity() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
        }));
        String expected = "{\"job\":[\"api\",\"worker\"]}";
        assertEquals(expected, readMetadata(mapper, Set.of(), "{\"job\":[\"worker\",\"api\",\"api\"]}"));
        assertEquals(expected, readMetadata(mapper, Set.of(), "{\"job\":[\"api\",\"worker\"]}"));
        assertEquals(expected, readMetadata(mapper, Set.of(), "{\"job\":[\"\",\"api\",\"worker\",null]}"));
        assertEquals("{\"job\":\"api\"}", readMetadata(mapper, Set.of(), "{\"job\":[\"api\",\"api\"]}"));
        assertEquals("{}", readMetadata(mapper, Set.of("job"), "{\"job\":[\"api\",\"worker\"]}"));
    }

    public void testMultipleNumericValuesKeepMappedPrecision() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("sequence").field("type", "long").field("time_series_dimension", true).endObject();
            b.startObject("unsigned").field("type", "unsigned_long").field("time_series_dimension", true).endObject();
        }));
        assertEquals(
            "{\"sequence\":[\"-2\",\"1\",\"10\"],\"unsigned\":[\"9007199254740993\",\"18446744073709551615\"]}",
            readMetadata(mapper, Set.of(), "{\"sequence\":[10,-2,1],\"unsigned\":[18446744073709551615,9007199254740993]}")
        );
    }

    public void testMultipleBooleanAndIpValues() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("enabled").field("type", "boolean").field("time_series_dimension", true).endObject();
            b.startObject("address").field("type", "ip").field("time_series_dimension", true).endObject();
        }));
        assertEquals(
            "{\"address\":[\"192.0.2.1\",\"2001:db8::1\"],\"enabled\":[\"false\",\"true\"]}",
            readMetadata(mapper, Set.of(), "{\"enabled\":[true,false],\"address\":[\"2001:db8::1\",\"192.0.2.1\"]}")
        );
    }

    public void testMultipleFlattenedValues() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("tags").field("type", "flattened").array("time_series_dimensions", "job").endObject();
        }));
        String input = "{\"tags\":{\"job\":[\"worker\",\"api\"],\"ignored\":[\"one\",\"two\"]}}";
        assertEquals("{\"tags.job\":[\"api\",\"worker\"]}", readMetadata(mapper, Set.of(), input));
        assertEquals("{}", readMetadata(mapper, Set.of("tags.job"), input));
    }

    public void testAmbiguousLabelNamesAreRejected() throws IOException {
        var mapper = createMapperService(mapping(b -> {
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.startObject("labels").startObject("properties");
            b.startObject("job").field("type", "keyword").field("time_series_dimension", true).endObject();
            b.endObject().endObject();
        }));
        var error = expectThrows(IllegalArgumentException.class, () -> readMetadata(mapper, Set.of(), "{}"));
        assertThat(error.getMessage(), containsString("Multiple dimension fields resolve to time-series label [job]"));
        for (Set<String> excluded : Set.of(Set.of("job"), Set.of("labels.job"), Set.of("job", "labels.job"))) {
            error = expectThrows(IllegalArgumentException.class, () -> readMetadata(mapper, excluded, "{}"));
            assertThat(error.getMessage(), containsString("Multiple dimension fields resolve to time-series label [job]"));
        }
    }

    private String readMetadata(MapperService mapper, Set<String> excluded, String json) throws IOException {
        var parsed = mapper.documentMapper().parse(source(json == null ? "{}" : json));
        var blocks = TestBlockFactory.getNonBreakingInstance();
        var factory = loaderFactory(blocks);
        var result = new AtomicReference<String>();
        withLuceneIndex(mapper, writer -> writer.addDocument(parsed.rootDoc()), reader -> {
            var loader = TimeSeriesMetadataBlockLoader.create(createSearchExecutionContext(mapper, new IndexSearcher(reader)), excluded);
            assertEquals(StoredFieldsSpec.NO_REQUIREMENTS, loader.rowStrideStoredFieldSpec());
            var leaf = reader.leaves().getFirst();
            try (
                var values = loader.columnAtATimeReader(leaf).apply(blocks.breaker());
                var output = (BytesRefBlock) values.read(factory, docs(0), 0, false)
            ) {
                result.set(metadata(output));
            }
            try (var values = loader.rowStrideReader(blocks.breaker(), leaf)) {
                if (values != null) {
                    try (var builder = loader.builder(factory, 1)) {
                        values.read(0, null, builder);
                        try (var output = (BytesRefBlock) builder.build()) {
                            assertEquals(result.get(), metadata(output));
                        }
                    }
                }
            }
        });
        return result.get();
    }

    private static BlockLoader.BlockFactory loaderFactory(BlockFactory blocks) {
        return new DelegatingBlockLoaderFactory(blocks) {
            @Override
            public BlockLoader.Block constantNulls(int count) {
                return blocks.newConstantNullBlock(count);
            }
        };
    }

    private static String metadata(BytesRefBlock output) throws IOException {
        assertEquals(1, output.getPositionCount());
        assertEquals(1, output.getValueCount(0));
        BytesRef bytes = output.getBytesRef(0, new BytesRef());
        assertNotNull(TimeSeriesMetadata.read(bytes));
        return bytes.utf8ToString();
    }

    private static BlockLoader.Docs docs(int... ids) {
        return new BlockLoader.Docs() {
            @Override
            public int count() {
                return ids.length;
            }

            @Override
            public int get(int i) {
                return ids[i];
            }

            @Override
            public boolean mayContainDuplicates() {
                return true;
            }
        };
    }
}
