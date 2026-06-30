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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.WarningSourceLocation;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.SpatialCoordinateTypes;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SequencedMap;

import static org.hamcrest.Matchers.is;

public class IpLocationFunctionBridgeTests extends AbstractCompoundOutputEvaluatorTests {

    private static final String KNOWN_IP = "89.160.20.128";
    private static final String UNKNOWN_IP = "127.0.0.1";
    private static final String SECOND_IP = "2.125.160.216";
    private static final String DATABASE_FILE = "GeoLite2-City.mmdb";

    private static final SequencedMap<String, Class<?>> TEST_FIELDS;
    static {
        LinkedHashMap<String, Class<?>> m = new LinkedHashMap<>();
        m.put("country_iso_code", String.class);
        m.put("country_name", String.class);
        m.put("continent_name", String.class);
        m.put("city_name", String.class);
        m.put("anonymous_vpn", Boolean.class);
        m.put("accuracy_radius", Integer.class);
        m.put("asn", Long.class);
        m.put("location", Object.class);
        TEST_FIELDS = Collections.unmodifiableSequencedMap(m);
    }

    /**
     * Stub that returns deterministic values for known IPs. Uses the {@link IpLocationInfoCollector}
     * callback interface exactly as the real MaxMind/ipinfo lookup implementations do.
     */
    private static IpDataLookup createFoundLookup() {
        return new StubIpDataLookup(true);
    }

    private static IpDataLookup createExpiredLookup() {
        return new StubIpDataLookup(false);
    }

    private static IpDataLookup createIoErrorLookup() {
        return new IpDataLookup() {
            @Override
            public Boolean lookup(String ip, IpLocationInfoCollector collector) throws IOException {
                throw new IOException("simulated read failure");
            }

            @Override
            public boolean isValid() {
                return true;
            }

            @Override
            public IpDataLookupInfo getInfo() {
                return STUB_INFO;
            }
        };
    }

    private IpDataLookup activeLookup = createFoundLookup();

    private final Warnings WARNINGS = Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new WarningSourceLocation() {
        @Override
        public int lineNumber() {
            return 1;
        }

        @Override
        public int columnNumber() {
            return 2;
        }

        @Override
        public String viewName() {
            return null;
        }

        @Override
        public String text() {
            return "ip_location_input";
        }
    });

    // --- AbstractCompoundOutputEvaluatorTests abstract method implementations ---

    @Override
    protected CompoundOutputEvaluator.OutputFieldsCollector createOutputFieldsCollector(List<String> requestedFields) {
        return new IpLocationFunctionBridge.IpLocationCollectorImpl(requestedFields, activeLookup, DATABASE_FILE);
    }

    @Override
    protected String collectorSimpleName() {
        return IpLocationFunctionBridge.IpLocationCollectorImpl.class.getSimpleName();
    }

    @Override
    protected Map<String, Class<?>> getSupportedOutputFieldMappings() {
        return TEST_FIELDS;
    }

    @Override
    protected List<String> getRequestedFieldsForSimple() {
        return List.of("country_iso_code", "country_name", "continent_name", "city_name");
    }

    @Override
    protected List<String> getSampleInputForSimple() {
        return List.of(KNOWN_IP, UNKNOWN_IP, SECOND_IP);
    }

    @Override
    protected List<Object[]> getExpectedOutputForSimple() {
        return List.of(
            new Object[] { "SE", null, "GB" },
            new Object[] { "Sweden", null, "United Kingdom" },
            new Object[] { "Europe", null, "Europe" },
            new Object[] { "Tumba", null, "Boxford" }
        );
    }

    // --- Focused test methods ---

    public void testFullOutput() {
        activeLookup = createFoundLookup();
        List<String> fields = List.of(
            "country_iso_code",
            "country_name",
            "continent_name",
            "city_name",
            "anonymous_vpn",
            "accuracy_radius",
            "asn",
            "location"
        );
        List<String> input = List.of(KNOWN_IP);
        List<?> expected = List.of("SE", "Sweden", "Europe", "Tumba", false, 50, 29518L, geoPointWkb(59.2, 17.8167));
        evaluateAndCompare(input, fields, expected);
    }

    public void testPartialFields() {
        activeLookup = createFoundLookup();
        List<String> fields = List.of("country_iso_code", "asn");
        List<String> input = List.of(KNOWN_IP);
        List<?> expected = List.of("SE", 29518L);
        evaluateAndCompare(input, fields, expected);
    }

    public void testNotFound() {
        activeLookup = createFoundLookup();
        List<String> fields = List.of("country_iso_code", "city_name", "asn", "anonymous_vpn");
        List<String> input = List.of(UNKNOWN_IP);
        List<?> expected = Arrays.asList(null, null, null, null);
        evaluateAndCompare(input, fields, expected);
    }

    public void testDbUnavailable() {
        List<String> fields = List.of("country_iso_code", "city_name", "asn", "anonymous_vpn", "location");
        var collector = new IpLocationFunctionBridge.IpLocationCollectorImpl(fields, null, DATABASE_FILE);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.REJECT,
            WARNINGS,
            collector
        );

        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block.Builder[] targetBlocks = new Block.Builder[fields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef(KNOWN_IP));
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                targetBlocks[0] = blockFactory.newBytesRefBlockBuilder(1);
                targetBlocks[1] = blockFactory.newBytesRefBlockBuilder(1);
                targetBlocks[2] = blockFactory.newLongBlockBuilder(1);
                targetBlocks[3] = blockFactory.newBooleanBlockBuilder(1);
                targetBlocks[4] = blockFactory.newBytesRefBlockBuilder(1);

                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

                try (Block b0 = targetBlocks[0].build(); Block b1 = targetBlocks[1].build()) {
                    String sentinel = "_ip_location_database_unavailable_" + DATABASE_FILE;
                    assertThat(((BytesRefBlock) b0).getBytesRef(0, new BytesRef()).utf8ToString(), is(sentinel));
                    assertThat(((BytesRefBlock) b1).getBytesRef(0, new BytesRef()).utf8ToString(), is(sentinel));
                }
                try (Block b2 = targetBlocks[2].build(); Block b3 = targetBlocks[3].build(); Block b4 = targetBlocks[4].build()) {
                    assertTrue("asn (long) should be null for unavailable DB", b2.isNull(0));
                    assertTrue("anonymous_vpn (boolean) should be null for unavailable DB", b3.isNull(0));
                    assertTrue("location (geo_point) should be null for unavailable DB", b4.isNull(0));
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
        assertWarnings(
            "Line 1:2: evaluation of [ip_location_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: org.elasticsearch.xpack.esql.evaluator.command.IpLocationFunctionBridge$IpLocationDatabaseUnavailableException: "
                + "IP location database ["
                + DATABASE_FILE
                + "] is not available on this node"
        );
    }

    public void testDbExpired() {
        List<String> fields = List.of("country_iso_code", "asn", "location");
        var collector = new IpLocationFunctionBridge.IpLocationCollectorImpl(fields, createExpiredLookup(), DATABASE_FILE);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.REJECT,
            WARNINGS,
            collector
        );

        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block.Builder[] targetBlocks = new Block.Builder[fields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef(KNOWN_IP));
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                targetBlocks[0] = blockFactory.newBytesRefBlockBuilder(1);
                targetBlocks[1] = blockFactory.newLongBlockBuilder(1);
                targetBlocks[2] = blockFactory.newBytesRefBlockBuilder(1);

                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

                try (Block b0 = targetBlocks[0].build()) {
                    assertThat(((BytesRefBlock) b0).getBytesRef(0, new BytesRef()).utf8ToString(), is("_ip_location_expired_database"));
                }
                try (Block b1 = targetBlocks[1].build(); Block b2 = targetBlocks[2].build()) {
                    assertTrue("asn (long) should be null for expired DB", b1.isNull(0));
                    assertTrue("location (geo_point) should be null for expired DB", b2.isNull(0));
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
        assertWarnings(
            "Line 1:2: evaluation of [ip_location_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: org.elasticsearch.xpack.esql.evaluator.command.IpLocationFunctionBridge$IpLocationDatabaseExpiredException: "
                + "IP location database ["
                + DATABASE_FILE
                + "] is expired"
        );
    }

    public void testIoException() {
        activeLookup = createIoErrorLookup();
        List<String> fields = List.of("country_iso_code", "city_name");
        var collector = new IpLocationFunctionBridge.IpLocationCollectorImpl(fields, activeLookup, DATABASE_FILE);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.REJECT,
            WARNINGS,
            collector
        );

        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block.Builder[] targetBlocks = new Block.Builder[fields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(1)) {
            inputBuilder.appendBytesRef(new BytesRef(KNOWN_IP));
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                targetBlocks[0] = blockFactory.newBytesRefBlockBuilder(1);
                targetBlocks[1] = blockFactory.newBytesRefBlockBuilder(1);

                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

                try (Block b0 = targetBlocks[0].build(); Block b1 = targetBlocks[1].build()) {
                    assertTrue("country_iso_code should be null on IOException", b0.isNull(0));
                    assertTrue("city_name should be null on IOException", b1.isNull(0));
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
        assertWarnings(
            "Line 1:2: evaluation of [ip_location_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: java.io.IOException: IP location lookup failed for database [" + DATABASE_FILE + "]"
        );
    }

    public void testGeoPointAxisOrder() {
        activeLookup = createFoundLookup();
        List<String> fields = List.of("location");
        List<String> input = List.of(KNOWN_IP);
        evaluateAndCompare(input, fields, List.of(geoPointWkb(59.2, 17.8167)));
    }

    public void testMultiValueReject() {
        activeLookup = createFoundLookup();
        List<String> fields = List.of("country_iso_code", "city_name");
        List<String> input = List.of(KNOWN_IP, SECOND_IP);
        List<Object[]> expected = Collections.nCopies(fields.size(), new Object[] { null });
        evaluateAndCompare(input, fields, expected, WARNINGS);
        assertWarnings(
            "Line 1:2: evaluation of [ip_location_input] failed, treating result as null. Only first 20 failures recorded.",
            "Line 1:2: java.lang.IllegalArgumentException: This command doesn't support multi-value input"
        );
    }

    public void testMultiValueTakeFirst() {
        activeLookup = createFoundLookup();
        List<String> fields = List.of("country_iso_code", "city_name");

        var collector = new IpLocationFunctionBridge.IpLocationCollectorImpl(fields, activeLookup, DATABASE_FILE);
        CompoundOutputEvaluator evaluator = new CompoundOutputEvaluator(
            DataType.TEXT,
            CompoundOutputEvaluator.MultiValueStrategy.TAKE_FIRST,
            Warnings.NOOP_WARNINGS,
            collector
        );

        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        Block.Builder[] targetBlocks = new Block.Builder[fields.size()];
        try (BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(2)) {
            inputBuilder.beginPositionEntry();
            inputBuilder.appendBytesRef(new BytesRef(KNOWN_IP));
            inputBuilder.appendBytesRef(new BytesRef(SECOND_IP));
            inputBuilder.endPositionEntry();
            try (BytesRefBlock inputBlock = inputBuilder.build()) {
                targetBlocks[0] = blockFactory.newBytesRefBlockBuilder(1);
                targetBlocks[1] = blockFactory.newBytesRefBlockBuilder(1);

                evaluator.computeRow(inputBlock, 0, targetBlocks, new BytesRef());

                try (Block b0 = targetBlocks[0].build(); Block b1 = targetBlocks[1].build()) {
                    assertFalse("country_iso_code should not be null", b0.isNull(0));
                    assertThat(((BytesRefBlock) b0).getBytesRef(0, new BytesRef()).utf8ToString(), is("SE"));
                    assertFalse("city_name should not be null", b1.isNull(0));
                    assertThat(((BytesRefBlock) b1).getBytesRef(0, new BytesRef()).utf8ToString(), is("Tumba"));
                }
            }
        } finally {
            Releasables.closeExpectNoException(targetBlocks);
        }
    }

    // --- Helpers ---

    /**
     * Encodes a geo point as WKB BytesRef for comparison in test assertions.
     */
    private static BytesRef geoPointWkb(double lat, double lon) {
        Point point = new Point(lon, lat);
        return SpatialCoordinateTypes.GEO.asWkb(point);
    }

    private static final IpDataLookupInfo STUB_INFO = new IpDataLookupInfo() {
        @Override
        public SequencedMap<String, Class<?>> getFields() {
            return TEST_FIELDS;
        }

        @Override
        public SequencedMap<String, Class<?>> getDefaultFields() {
            return TEST_FIELDS;
        }

        @Override
        public String getDatabaseType() {
            return "GeoLite2-City";
        }
    };

    /**
     * Stub lookup that returns deterministic values for known IPs via the collector interface.
     */
    private static class StubIpDataLookup implements IpDataLookup {
        private final boolean valid;

        StubIpDataLookup(boolean valid) {
            this.valid = valid;
        }

        @Override
        public Boolean lookup(String ip, IpLocationInfoCollector collector) {
            return switch (ip) {
                case KNOWN_IP -> {
                    collector.countryIsoCode("SE");
                    collector.countryName("Sweden");
                    collector.continentName("Europe");
                    collector.cityName("Tumba");
                    collector.anonymousVpn(false);
                    collector.accuracyRadius(50);
                    collector.asn(29518L);
                    collector.location(59.2, 17.8167);
                    yield true;
                }
                case SECOND_IP -> {
                    collector.countryIsoCode("GB");
                    collector.countryName("United Kingdom");
                    collector.continentName("Europe");
                    collector.cityName("Boxford");
                    collector.anonymousVpn(false);
                    collector.accuracyRadius(100);
                    collector.asn(2856L);
                    collector.location(51.75, -1.25);
                    yield true;
                }
                default -> false;
            };
        }

        @Override
        public boolean isValid() {
            return valid;
        }

        @Override
        public IpDataLookupInfo getInfo() {
            return STUB_INFO;
        }
    }
}
