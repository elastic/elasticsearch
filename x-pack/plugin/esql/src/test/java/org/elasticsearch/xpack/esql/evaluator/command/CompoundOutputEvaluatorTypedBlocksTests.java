/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.function.BiConsumer;
import java.util.function.ObjLongConsumer;

import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_BOOLEAN_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_GEO_POINT_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_LONG_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.booleanValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.geoPointValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.longValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for the typed block extensions in {@link CompoundOutputEvaluator}: boolean, long, geo_point output
 * and the {@link CompoundOutputEvaluator.MultiValueStrategy} behavior.
 */
public class CompoundOutputEvaluatorTypedBlocksTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    public void testBooleanOutput() {
        runSingleRow("name:alice;active:true", List.of("name", "active"), (blocks, scratch) -> {
            assertThat(((BytesRefBlock) blocks[0]).getBytesRef(0, scratch).utf8ToString(), is("alice"));
            assertThat(((BooleanBlock) blocks[1]).getBoolean(0), is(true));
        });
    }

    public void testLongOutput() {
        runSingleRow("name:bob;asn:12345", List.of("name", "asn"), (blocks, scratch) -> {
            assertThat(((BytesRefBlock) blocks[0]).getBytesRef(0, scratch).utf8ToString(), is("bob"));
            assertThat(((LongBlock) blocks[1]).getLong(0), is(12345L));
        });
    }

    public void testGeoPointRoundTrip() {
        double lat = 51.5;
        double lon = -0.1;
        runSingleRow("name:london;lat:51.5;lon:-0.1", List.of("name", "location"), (blocks, scratch) -> {
            assertThat(((BytesRefBlock) blocks[0]).getBytesRef(0, scratch).utf8ToString(), is("london"));
            BytesRef wkb = ((BytesRefBlock) blocks[1]).getBytesRef(0, new BytesRef());
            assertThat(wkb, notNullValue());
            assertThat("WKB for a 2D point must be 21 bytes", wkb.length, is(21));
            Point point = SpatialCoordinateTypes.GEO.wkbAsPoint(wkb);
            assertThat(point.getY(), closeTo(lat, 1e-10));
            assertThat(point.getX(), closeTo(lon, 1e-10));
        });
    }

    public void testAllTypesInOneRow() {
        runSingleRow("name:test;active:false;asn:99;lat:40.0;lon:2.0", List.of("name", "active", "asn", "location"), (blocks, scratch) -> {
            assertThat(((BytesRefBlock) blocks[0]).getBytesRef(0, scratch).utf8ToString(), is("test"));
            assertThat(((BooleanBlock) blocks[1]).getBoolean(0), is(false));
            assertThat(((LongBlock) blocks[2]).getLong(0), is(99L));
            Point point = SpatialCoordinateTypes.GEO.wkbAsPoint(((BytesRefBlock) blocks[3]).getBytesRef(0, new BytesRef()));
            assertThat(point.getY(), closeTo(40.0, 1e-10));
            assertThat(point.getX(), closeTo(2.0, 1e-10));
        });
    }

    public void testMultiValueRejectProducesNulls() {
        List<String> fields = List.of("name", "active");
        TypedTestCollector collector = new TypedTestCollector(fields);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.REJECT,
            Warnings.NOOP_WARNINGS,
            collector
        );
        Block.Builder[] targetBlocks = new Block.Builder[fields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(2)) {
            inputBuilder.beginPositionEntry();
            inputBuilder.appendBytesRef(new BytesRef("name:a;active:true"));
            inputBuilder.appendBytesRef(new BytesRef("name:b;active:false"));
            inputBuilder.endPositionEntry();
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                targetBlocks[0] = blockFactory.newBytesRefBlockBuilder(1);
                targetBlocks[1] = blockFactory.newBooleanBlockBuilder(1);

                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

                try (Block b0 = targetBlocks[0].build(); Block b1 = targetBlocks[1].build()) {
                    assertTrue("name field should be null", b0.isNull(0));
                    assertTrue("active field should be null", b1.isNull(0));
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }

    public void testMultiValueTakeFirstConsumesFirstValue() {
        List<String> fields = List.of("name", "active");
        TypedTestCollector collector = new TypedTestCollector(fields);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.TAKE_FIRST,
            Warnings.NOOP_WARNINGS,
            collector
        );
        Block.Builder[] targetBlocks = new Block.Builder[fields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(2)) {
            inputBuilder.beginPositionEntry();
            inputBuilder.appendBytesRef(new BytesRef("name:first;active:true"));
            inputBuilder.appendBytesRef(new BytesRef("name:second;active:false"));
            inputBuilder.endPositionEntry();
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                targetBlocks[0] = blockFactory.newBytesRefBlockBuilder(1);
                targetBlocks[1] = blockFactory.newBooleanBlockBuilder(1);

                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

                BytesRef scratch = new BytesRef();
                try (Block b0 = targetBlocks[0].build(); Block b1 = targetBlocks[1].build()) {
                    assertFalse("name field should not be null", b0.isNull(0));
                    assertThat(((BytesRefBlock) b0).getBytesRef(0, scratch).utf8ToString(), is("first"));
                    assertFalse("active field should not be null", b1.isNull(0));
                    assertThat(((BooleanBlock) b1).getBoolean(0), is(true));
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }

    public void testTakeFirstWithSingleValueBehavesSame() {
        List<String> fields = List.of("name");
        TypedTestCollector collector = new TypedTestCollector(fields);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.TAKE_FIRST,
            Warnings.NOOP_WARNINGS,
            collector
        );
        Block.Builder[] targetBlocks = new Block.Builder[1];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef("name:only"));
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                targetBlocks[0] = blockFactory.newBytesRefBlockBuilder(1);
                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());
                try (Block b0 = targetBlocks[0].build()) {
                    assertFalse(b0.isNull(0));
                    assertThat(((BytesRefBlock) b0).getBytesRef(0, new BytesRef()).utf8ToString(), is("only"));
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }

    // ---- Helpers ----

    @FunctionalInterface
    interface BlockAssertion {
        void assertBlocks(Block[] blocks, BytesRef scratch);
    }

    private void runSingleRow(String input, List<String> fields, BlockAssertion assertion) {
        TypedTestCollector collector = new TypedTestCollector(fields);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.REJECT,
            Warnings.NOOP_WARNINGS,
            collector
        );
        Map<String, Class<?>> supportedFields = TypedTestCollector.SUPPORTED_FIELDS;
        Block.Builder[] targetBlocks = new Block.Builder[fields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef(input));
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                for (int i = 0; i < fields.size(); i++) {
                    Class<?> type = supportedFields.get(fields.get(i));
                    if (type == Long.class) {
                        targetBlocks[i] = blockFactory.newLongBlockBuilder(1);
                    } else if (type == Boolean.class) {
                        targetBlocks[i] = blockFactory.newBooleanBlockBuilder(1);
                    } else {
                        targetBlocks[i] = blockFactory.newBytesRefBlockBuilder(1);
                    }
                }
                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());
                Block[] builtBlocks = new Block[fields.size()];
                try {
                    for (int i = 0; i < fields.size(); i++) {
                        builtBlocks[i] = targetBlocks[i].build();
                    }
                    assertion.assertBlocks(builtBlocks, new BytesRef());
                } finally {
                    Releasables.closeExpectNoException(builtBlocks);
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }

    /**
     * A test collector that parses "key:value-key:value" pairs and supports String, Boolean, Long, and geo-point outputs.
     */
    static class TypedTestCollector extends CompoundOutputEvaluator.OutputFieldsCollector {

        static final Map<String, Class<?>> SUPPORTED_FIELDS = Map.of(
            "name",
            String.class,
            "active",
            Boolean.class,
            "asn",
            Long.class,
            "location",
            Object.class
        );

        private BiConsumer<CompoundOutputEvaluator.RowOutput, String> nameCollector = NOOP_STRING_COLLECTOR;
        private CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> activeCollector = NOOP_BOOLEAN_COLLECTOR;
        private ObjLongConsumer<CompoundOutputEvaluator.RowOutput> asnCollector = NOOP_LONG_COLLECTOR;
        private CompoundOutputEvaluator.GeoPointCollector locationCollector = NOOP_GEO_POINT_COLLECTOR;

        TypedTestCollector(SequencedCollection<String> outputFields) {
            super(outputFields.size());
            int index = 0;
            for (String field : outputFields) {
                switch (field) {
                    case "name" -> nameCollector = stringValueCollector(index);
                    case "active" -> activeCollector = booleanValueCollector(index);
                    case "asn" -> asnCollector = longValueCollector(index);
                    case "location" -> locationCollector = geoPointValueCollector(index);
                    default -> throw new IllegalArgumentException("Unknown field: " + field);
                }
                index++;
            }
        }

        @Override
        protected void evaluate(String input) {
            String[] parts = input.split(";");
            Double lat = null;
            Double lon = null;
            for (String part : parts) {
                String[] kv = part.split(":", 2);
                if (kv.length != 2) continue;
                switch (kv[0]) {
                    case "name" -> nameCollector.accept(rowOutput, kv[1]);
                    case "active" -> activeCollector.accept(rowOutput, Booleans.parseBoolean(kv[1]));
                    case "asn" -> asnCollector.accept(rowOutput, Long.parseLong(kv[1]));
                    case "lat" -> lat = Double.parseDouble(kv[1]);
                    case "lon" -> lon = Double.parseDouble(kv[1]);
                }
            }
            if (lat != null && lon != null) {
                locationCollector.accept(rowOutput, lat, lon);
            }
        }
    }
}
