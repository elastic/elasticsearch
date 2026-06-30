/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.datasources.spi.ColumnExtractor;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

/**
 * Unit tests for {@link SynthesizeExternalSource}, the per-row composer of the {@code _source}
 * metadata column for external rows.
 */
public class SynthesizeExternalSourceTests extends ESTestCase {

    private final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    /**
     * Regression: user data columns whose names happen to start with an underscore
     * ({@code _corrupt_record}, {@code _status}, ...) must reach the rendered {@code _source}.
     * A prior implementation used a leading-underscore prefix filter to drop synthetic channels
     * which silently swallowed these legitimate column names.
     */
    public void testUserColumnsStartingWithUnderscoreAreIncluded() throws Exception {
        try (
            IntBlock idCol = intBlock(42);
            BytesRefBlock corrupt = bytesRefBlock("not valid json");
            BytesRefBlock status = bytesRefBlock("ok")
        ) {
            String[] names = { "_corrupt_record", "id", "_status" };
            DataType[] types = { DataType.KEYWORD, DataType.INTEGER, DataType.KEYWORD };
            Block[] blocks = { corrupt, idCol, status };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue("user column _corrupt_record must appear in _source: " + json, json.contains("_corrupt_record"));
                assertTrue("user column _status must appear in _source: " + json, json.contains("_status"));
                assertTrue("data column id must appear in _source: " + json, json.contains("\"id\""));
            }
        }
    }

    /**
     * Framework-injected synthetic channel ({@link ColumnExtractor#ROW_POSITION_COLUMN}) must
     * NOT appear in the rendered {@code _source}.
     */
    public void testRowPositionColumnIsExcluded() throws Exception {
        try (IntBlock idCol = intBlock(7); LongBlock rowPos = longBlock(123L)) {
            String[] names = { "id", ColumnExtractor.ROW_POSITION_COLUMN };
            DataType[] types = { DataType.INTEGER, DataType.LONG };
            Block[] blocks = { idCol, rowPos };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue("data column id must appear in _source: " + json, json.contains("\"id\""));
                assertFalse(
                    "synthetic _rowPosition must be excluded from _source: " + json,
                    json.contains(ColumnExtractor.ROW_POSITION_COLUMN)
                );
            }
        }
    }

    /**
     * Block subtypes not enumerated by the value-rendering switch (here: {@link CompositeBlock})
     * must fail loudly. Falling through to {@code toString()} would emit implementation-specific
     * debug text into user-visible JSON; preferring an explicit failure forces the new type's
     * handling to be added intentionally rather than discovered as corrupt {@code _source}.
     */
    public void testUnsupportedBlockTypeRejected() throws Exception {
        try (IntBlock inner = intBlock(1)) {
            inner.incRef();
            try (CompositeBlock composite = new CompositeBlock(new Block[] { inner })) {
                String[] names = { "weird" };
                DataType[] types = { DataType.KEYWORD };
                Block[] blocks = { composite };
                UnsupportedOperationException thrown = expectThrows(
                    UnsupportedOperationException.class,
                    () -> SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)
                );
                // The error originates inside CompositeBlock.getValueCount, which BlockUtils
                // calls during the toJavaObject dispatch — "Composite block" is its message.
                assertTrue(
                    "exception message must identify the rejected block: " + thrown.getMessage(),
                    thrown.getMessage().contains("Composite block") || thrown.getMessage().contains("COMPOSITE")
                );
            }
        }
    }

    /**
     * Multi-value columns must round-trip into {@code _source} as a JSON array of UTF-8 strings,
     * not as a base64-encoded scalar or as the first value only.
     * {@link org.elasticsearch.compute.data.BlockUtils#toJavaObject} returns an {@code ArrayList}
     * for multi-value rows; the synthesizer's {@code renderValue} list branch must render each
     * element.
     */
    public void testMultiValueBytesRefRoundTripsAsJsonArray() throws Exception {
        try (IntBlock idCol = intBlock(7); BytesRefBlock tags = multiValueBytesRefBlock("alpha", "beta", "gamma")) {
            String[] names = { "id", "tags" };
            DataType[] types = { DataType.INTEGER, DataType.KEYWORD };
            Block[] blocks = { idCol, tags };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue(
                    "multi-value tags must render as a JSON array of UTF-8 strings: " + json,
                    json.contains("[\"alpha\",\"beta\",\"gamma\"]")
                );
                assertFalse("multi-value tags must not render as a single string: " + json, json.contains("\"tags\":\"alpha\""));
            }
        }
    }

    /**
     * IP and VERSION columns carry wire-encoded {@link BytesRef}s (16-byte InetAddressPoint /
     * semver wire bytes); rendering them with {@code utf8ToString} emits garbage into
     * {@code _source}. They must decode the same way the response layer does.
     */
    public void testIpAndVersionRenderDecoded() throws Exception {
        try (
            BytesRefBlock ip = bytesRefBlock(EsqlDataTypeConverter.stringToIP("192.168.0.1"));
            BytesRefBlock version = bytesRefBlock(EsqlDataTypeConverter.stringToVersion("1.2.3"))
        ) {
            String[] names = { "client_ip", "app_version" };
            DataType[] types = { DataType.IP, DataType.VERSION };
            Block[] blocks = { ip, version };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue("IP must render as dotted string: " + json, json.contains("\"client_ip\":\"192.168.0.1\""));
                assertTrue("VERSION must render as semver string: " + json, json.contains("\"app_version\":\"1.2.3\""));
            }
        }
    }

    /**
     * DATETIME / DATE_NANOS blocks carry epoch longs; {@code _source} must render the same UTC
     * ISO-8601 strings the response layer emits for those columns, not the raw numbers.
     */
    public void testDateTypesRenderAsIsoStrings() throws Exception {
        try (LongBlock millis = longBlock(1700000000000L); LongBlock nanos = longBlock(1700000000123456789L)) {
            String[] names = { "ts", "ts_nanos" };
            DataType[] types = { DataType.DATETIME, DataType.DATE_NANOS };
            Block[] blocks = { millis, nanos };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue("DATETIME must render as ISO-8601: " + json, json.contains("\"ts\":\"2023-11-14T22:13:20.000Z\""));
                assertTrue("DATE_NANOS must render as ISO-8601: " + json, json.contains("\"ts_nanos\":\"2023-11-14T22:13:20.123456789Z\""));
            }
        }
    }

    /**
     * UNSIGNED_LONG blocks carry ESQL's sign-flipped long encoding; {@code _source} must decode
     * to the numeric value — a raw pass-through would sign-flip every value ≥ 2^63.
     */
    public void testUnsignedLongRendersDecodedNumber() throws Exception {
        try (LongBlock ul = longBlock(NumericUtils.asLongUnsigned(NumericUtils.UNSIGNED_LONG_MAX))) {
            String[] names = { "counter" };
            DataType[] types = { DataType.UNSIGNED_LONG };
            Block[] blocks = { ul };
            try (BytesRefBlock source = SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)) {
                String json = bytesAt(source, 0);
                assertTrue("UNSIGNED_LONG must decode to its numeric value: " + json, json.contains("\"counter\":18446744073709551615"));
            }
        }
    }

    /**
     * Types no reader can emit today must fail loud rather than render an undefined shape; the
     * new type's {@code _source} rendering has to be added intentionally.
     */
    public void testUnhandledDataTypeFailsLoud() throws Exception {
        for (DataType unhandled : new DataType[] { DataType.GEO_POINT, DataType.AGGREGATE_METRIC_DOUBLE }) {
            try (BytesRefBlock payload = bytesRefBlock("opaque payload")) {
                String[] names = { "weird" };
                DataType[] types = { unhandled };
                Block[] blocks = { payload };
                EsqlIllegalArgumentException thrown = expectThrows(
                    EsqlIllegalArgumentException.class,
                    () -> SynthesizeExternalSource.composePage(names, types, blocks, 1, blockFactory)
                );
                assertTrue(
                    "message must name the unhandled type: " + thrown.getMessage(),
                    thrown.getMessage().contains(unhandled.typeName())
                );
            }
        }
    }

    private BytesRefBlock multiValueBytesRefBlock(String... values) {
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(1)) {
            b.beginPositionEntry();
            for (String v : values) {
                b.appendBytesRef(new BytesRef(v));
            }
            b.endPositionEntry();
            return b.build();
        }
    }

    private IntBlock intBlock(int value) {
        try (IntBlock.Builder b = blockFactory.newIntBlockBuilder(1)) {
            b.appendInt(value);
            return b.build();
        }
    }

    private LongBlock longBlock(long value) {
        try (LongBlock.Builder b = blockFactory.newLongBlockBuilder(1)) {
            b.appendLong(value);
            return b.build();
        }
    }

    private BytesRefBlock bytesRefBlock(String value) {
        return bytesRefBlock(new BytesRef(value));
    }

    private BytesRefBlock bytesRefBlock(BytesRef value) {
        try (BytesRefBlock.Builder b = blockFactory.newBytesRefBlockBuilder(1)) {
            b.appendBytesRef(value);
            return b.build();
        }
    }

    private static String bytesAt(BytesRefBlock block, int row) {
        BytesRef scratch = new BytesRef();
        BytesRef ref = block.getBytesRef(block.getFirstValueIndex(row), scratch);
        return ref.utf8ToString();
    }
}
