/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.Text;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class TsidBuilderTests extends ESTestCase {

    private static IndexVersion randomMultiplePrefixBytesVersion() {
        return IndexVersionUtils.randomPreviousCompatibleVersion(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG);
    }

    private static IndexVersion randomSinglePrefixByteVersion() {
        return IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_SINGLE_PREFIX_BYTE_FEATURE_FLAG);
    }

    /** A version with the single prefix byte but without the prefixed-dimensions hash. */
    private static IndexVersion randomSinglePrefixByteOnlyVersion() {
        return IndexVersionUtils.randomVersionBetween(
            IndexVersions.TSID_SINGLE_PREFIX_BYTE,
            IndexVersionUtils.getPreviousVersion(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH)
        );
    }

    private static IndexVersion randomPrefixedDimensionsHashVersion() {
        return IndexVersionUtils.randomVersionOnOrAfter(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH);
    }

    /** The value the OTel metrics templates set {@link IndexSettings#TIME_SERIES_TSID_PREFIXED_DIMENSIONS} to. */
    private static final List<String> OTEL_PREFIXED_DIMENSIONS = List.of("resource.attributes.*");

    /** The layout of an OTel metrics index created at {@code version}. */
    private static TsidLayout otelLayout(IndexVersion version) {
        return new TsidLayout(version, OTEL_PREFIXED_DIMENSIONS);
    }

    private static String hex(BytesRef tsid) {
        return HexFormat.of().formatHex(tsid.bytes, tsid.offset, tsid.length);
    }

    private static byte[] slice(BytesRef tsid, int from, int to) {
        return Arrays.copyOfRange(tsid.bytes, tsid.offset + from, tsid.offset + to);
    }

    /** An OTel-shaped series: metric, two resource attributes, and dimensions outside the resource. */
    private static TsidBuilder otelSeries(String host, String service, String operation) {
        return TsidBuilder.newBuilder()
            .addStringDimension(TsidBuilder.OTEL_METRIC_FIELD, "random1")
            .addStringDimension("resource.attributes.host.name", host)
            .addStringDimension("resource.attributes.service.name", service)
            .addStringDimension("attributes.operation", operation)
            .addStringDimension("scope.name", "scope")
            .addIntDimension("attributes.port", 8080);
    }

    /** A Prometheus-shaped series: the metric name label, the scrape target labels, and another label. */
    private static TsidBuilder prometheusSeries(String job, String instance, String method) {
        return TsidBuilder.newBuilder()
            .addStringDimension(TsidBuilder.PROMETHEUS_LABEL_FIELD, "http_requests_total")
            .addStringDimension("labels.job", job)
            .addStringDimension("labels.instance", instance)
            .addStringDimension("labels.method", method);
    }

    /**
     * A builder reused via {@link TsidBuilder#reset()} — as {@code DimensionsExtractor} does on the
     * batch path — must produce exactly what a fresh builder would, since state carried across builds
     * (the hasher, and the multi-byte layout's value-similarity staging buffer) outlives a single
     * build. The second shape is array-valued, so its dimension count exceeds the number of
     * value-similarity bytes it emits and the backing array is deliberately larger than the returned
     * slice.
     */
    public void testReusedBuilderMatchesFreshBuilder() {
        for (IndexVersion version : List.of(randomMultiplePrefixBytesVersion(), randomSinglePrefixByteVersion())) {
            TsidBuilder reused = TsidBuilder.newBuilder();

            // Fills every value-similarity slot: 5 distinct paths against a cap of 4.
            BytesRef first = addDistinctPaths(reused).buildTsid(version);
            assertThat(first, equalTo(addDistinctPaths(TsidBuilder.newBuilder()).buildTsid(version)));

            // 5 dimensions collapsing onto 1 path, so only one value-similarity byte is emitted.
            reused.reset();
            BytesRef second = addRepeatedPath(reused).buildTsid(version);
            assertThat(second, equalTo(addRepeatedPath(TsidBuilder.newBuilder()).buildTsid(version)));
            assertThat(second, not(equalTo(first)));

            // And back again, so the shrink/grow transition is covered in both directions.
            reused.reset();
            assertThat(addDistinctPaths(reused).buildTsid(version), equalTo(first));
        }
    }

    private static TsidBuilder addDistinctPaths(TsidBuilder builder) {
        return builder.addStringDimension("a", "1")
            .addStringDimension("b", "2")
            .addStringDimension("c", "3")
            .addStringDimension("d", "4")
            .addStringDimension("e", "5");
    }

    private static TsidBuilder addRepeatedPath(TsidBuilder builder) {
        return builder.addStringDimension("a", "1")
            .addStringDimension("a", "2")
            .addStringDimension("a", "3")
            .addStringDimension("a", "4")
            .addStringDimension("a", "5");
    }

    public void testAddDimensions() {
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension("test_string", "hello")
            .addBooleanDimension("test_bool", true)
            .addIntDimension("test_int", 42)
            .addLongDimension("test_long", 123456789L)
            .addDoubleDimension("test_double", 3.14159)
            .addStringDimension("test_array", "value1")
            .addStringDimension("test_array", "value2");

        // if these change, we'll need a new index version
        // because it means existing time series will get a new _tsid and will be routed to a different shard
        assertThat(builder.hash().toString(), equalTo("0xd4de1356065d297a2be489781e15d256"));
        BytesRef legacyTsid = builder.buildTsid(randomMultiplePrefixBytesVersion());
        assertThat(legacyTsid.length, equalTo(21));
        assertThat(
            HexFormat.of().formatHex(legacyTsid.bytes, legacyTsid.offset, legacyTsid.length),
            equalTo("bfa0a8d66356d2151e7889e42b7a295d065613ded4")
        );
        IndexVersion newVersion = randomSinglePrefixByteVersion();
        BytesRef newTsid = builder.buildTsid(newVersion);
        if (TsidBuilder.useSingleBytePrefixLayout(newVersion)) {
            assertThat(
                HexFormat.of().formatHex(newTsid.bytes, newTsid.offset, newTsid.length),
                equalTo("bfd2151e7889e42b7a295d065613ded4")
            );
        } else {
            assertThat(newTsid, equalTo(legacyTsid));
        }
    }

    public void testArray() {
        TsidBuilder builder = TsidBuilder.newBuilder().addStringDimension("test_non_array", "value");

        int arrayValues = randomIntBetween(32, 64);
        for (int i = 0; i < arrayValues; i++) {
            builder.addStringDimension("_test_large_array", "value_" + i);
        }
        assertThat(builder.buildTsid(randomMultiplePrefixBytesVersion()).length, equalTo(19));
        IndexVersion singleVersion = randomSinglePrefixByteVersion();
        if (TsidBuilder.useSingleBytePrefixLayout(singleVersion)) {
            assertThat(builder.buildTsid(singleVersion).length, equalTo(16));
        } else {
            assertThat(builder.buildTsid(singleVersion).length, equalTo(19));
        }
    }

    public void testOrderingOfDifferentFieldsDoesNotMatter() {
        assertEqualBuilders(
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("baz", "qux"),
            TsidBuilder.newBuilder().addStringDimension("baz", "qux").addStringDimension("foo", "bar")
        );
    }

    public void testOrderingOfMultiFieldsMatters() {
        IndexVersion oldVersion = randomMultiplePrefixBytesVersion();
        assertThat(
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("foo", "baz").buildTsid(oldVersion),
            not(equalTo(TsidBuilder.newBuilder().addStringDimension("foo", "baz").addStringDimension("foo", "bar").buildTsid(oldVersion)))
        );
        IndexVersion newVersion = randomSinglePrefixByteVersion();
        assertThat(
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("foo", "baz").buildTsid(newVersion),
            not(equalTo(TsidBuilder.newBuilder().addStringDimension("foo", "baz").addStringDimension("foo", "bar").buildTsid(newVersion)))
        );
    }

    public void testAddStringDimension() {
        String stringValue = randomUnicodeOfLengthBetween(0, 1024);
        BytesRef bytesRef = new BytesRef(stringValue);
        byte[] utf8Bytes = stringValue.getBytes(StandardCharsets.UTF_8);
        assertEqualBuilders(
            TsidBuilder.newBuilder().addStringDimension("test_string", stringValue),
            TsidBuilder.newBuilder().addStringDimension("test_string", new Text(stringValue).bytes()),
            TsidBuilder.newBuilder().addStringDimension("test_string", bytesRef.bytes, bytesRef.offset, bytesRef.length),
            TsidBuilder.newBuilder().addStringDimension("test_string", utf8Bytes, 0, utf8Bytes.length)
        );
    }

    private static void assertEqualBuilders(TsidBuilder... tsidBuilders) {
        IndexVersion version = randomBoolean() ? randomMultiplePrefixBytesVersion() : randomSinglePrefixByteVersion();
        assertThat(Arrays.stream(tsidBuilders).map(builder -> builder.buildTsid(version)).distinct().toList(), hasSize(1));
        assertThat(Arrays.stream(tsidBuilders).map(TsidBuilder::hash).distinct().toList(), hasSize(1));
        assertThat(tsidBuilders[0].buildTsid(version), notNullValue());
        assertThat(tsidBuilders[0].buildTsid(version).length, greaterThan(0));
    }

    public void testAddAll() {
        TsidBuilder builder1 = TsidBuilder.newBuilder().addStringDimension("foo", "bar");
        TsidBuilder builder2 = TsidBuilder.newBuilder().addStringDimension("baz", "qux");
        assertEqualBuilders(
            TsidBuilder.newBuilder().addAll(builder1).addAll(builder2),
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("baz", "qux")
        );
    }

    public void testAddAllWithNullOrEmpty() {
        assertEqualBuilders(
            TsidBuilder.newBuilder().addIntDimension("test", 42),
            TsidBuilder.newBuilder().addIntDimension("test", 42).addAll(null).addAll(TsidBuilder.newBuilder())
        );
    }

    public void testExceptionWhenNoDimensions() {
        TsidBuilder builder = TsidBuilder.newBuilder();
        assertThat(builder.hash(), equalTo(new MurmurHash3.Hash128()));
        for (IndexVersion version : List.of(randomMultiplePrefixBytesVersion(), randomSinglePrefixByteVersion())) {
            IllegalArgumentException tsidException = expectThrows(IllegalArgumentException.class, () -> builder.buildTsid(version));
            assertTrue(tsidException.getMessage().contains("Dimensions are empty"));
        }
    }

    public void testTsidMinSize() {
        TsidBuilder builder = TsidBuilder.newBuilder().addIntDimension("test_int", 42);
        assertThat(builder.buildTsid(randomMultiplePrefixBytesVersion()).length, equalTo(18));
    }

    public void testTsidMaxSize() {
        TsidBuilder tsidBuilder = TsidBuilder.newBuilder();
        int dimensions = randomIntBetween(4, 64);
        for (int i = 0; i < dimensions; i++) {
            tsidBuilder.addStringDimension("dimension_" + i, "value_" + i);
        }
        assertEquals(21, tsidBuilder.buildTsid(randomMultiplePrefixBytesVersion()).length);
    }

    public void testOtelSchema() {
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension("_metric_names_hash", "random1")
            .addBooleanDimension("test_bool", true)
            .addIntDimension("test_int", 42)
            .addLongDimension("test_long", 123456789L)
            .addDoubleDimension("test_double", 3.14159)
            .addStringDimension("test_array", "value1")
            .addStringDimension("test_array", "value2");
        BytesRef oldTsid = builder.buildTsid(randomMultiplePrefixBytesVersion());
        IndexVersion newVersion = randomSinglePrefixByteVersion();
        BytesRef newTsid = builder.buildTsid(newVersion);
        assertThat(
            HexFormat.of().formatHex(oldTsid.bytes, oldTsid.offset, oldTsid.length),
            equalTo("01e3a0a8d693dbccd8eed09bb80b82b55d9756c7a6")
        );
        if (TsidBuilder.useSingleBytePrefixLayout(newVersion)) {
            assertThat(
                HexFormat.of().formatHex(newTsid.bytes, newTsid.offset, newTsid.length),
                equalTo("e3dbccd8eed09bb80b82b55d9756c7a6")
            );
        } else {
            assertThat(newTsid, equalTo(oldTsid));
        }
    }

    public void testPrometheusSchema() {
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension("labels.__name__", "random1")
            .addBooleanDimension("test_bool", true)
            .addIntDimension("test_int", 42)
            .addLongDimension("test_long", 123456789L)
            .addDoubleDimension("test_double", 3.14159)
            .addStringDimension("test_array", "value1")
            .addStringDimension("test_array", "value2");
        BytesRef oldTsid = builder.buildTsid(randomMultiplePrefixBytesVersion());
        assertThat(
            HexFormat.of().formatHex(oldTsid.bytes, oldTsid.offset, oldTsid.length),
            equalTo("afe3a0a8d67821311bea0fc3c9cbd40c8047c484aa")
        );
        IndexVersion version = randomSinglePrefixByteVersion();
        BytesRef newTsid = builder.buildTsid(version);
        if (TsidBuilder.useSingleBytePrefixLayout(version)) {
            assertThat(
                HexFormat.of().formatHex(newTsid.bytes, newTsid.offset, newTsid.length),
                equalTo("e321311bea0fc3c9cbd40c8047c484aa")
            );
        } else {
            assertThat(newTsid, equalTo(oldTsid));
        }
    }

    public void testEnableSinglePrefixByte() {
        assertTrue(TsidBuilder.useSingleBytePrefixLayout(IndexVersion.current()));
    }

    public void testUsePrefixedDimensionsHash() {
        assertTrue(TsidBuilder.usePrefixedDimensionsHash(IndexVersion.current()));
        assertTrue(TsidBuilder.usePrefixedDimensionsHash(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH));
        IndexVersion before = randomSinglePrefixByteOnlyVersion();
        assertFalse(TsidBuilder.usePrefixedDimensionsHash(before));
        // The prefixed-dimensions hash is a refinement of the single-prefix-byte layout, never of the legacy one.
        assertTrue(TsidBuilder.useSingleBytePrefixLayout(IndexVersions.TSID_PREFIXED_DIMENSIONS_HASH));
    }

    /**
     * The prefixed-dimensions hash replaces bytes 1-2 and nothing else: byte 0 is still the metric similarity byte
     * and the tail is still the full hash.
     */
    public void testPrefixedDimensionsHashLayout() {
        TsidBuilder builder = otelSeries("host-1", "service-a", "op-1");
        BytesRef before = builder.buildTsid(randomSinglePrefixByteOnlyVersion());
        BytesRef after = builder.buildTsid(otelLayout(randomPrefixedDimensionsHashVersion()));
        assertThat(after.length, equalTo(16));
        assertThat(after.bytes[after.offset], equalTo(before.bytes[before.offset]));
        assertThat(slice(after, 3, 16), equalTo(slice(before, 3, 16)));
        assertThat(slice(after, 1, 3), not(equalTo(slice(before, 1, 3))));
        // if these change, we'll need a new index version
        // because it means existing time series will get a new _tsid and will be routed to a different shard
        assertThat(hex(before), equalTo("e385eaf31474d47326e3601d8140f616"));
        assertThat(hex(after), equalTo("e31226f31474d47326e3601d8140f616"));
    }

    /** Series of one metric and resource sort next to each other; series of another resource do not. */
    public void testPrefixedDimensionsHashClustersSeriesByResource() {
        TsidLayout layout = otelLayout(randomPrefixedDimensionsHashVersion());
        BytesRef sameResource1 = otelSeries("host-1", "service-a", "op-1").buildTsid(layout);
        BytesRef sameResource2 = otelSeries("host-1", "service-a", "op-2").buildTsid(layout);
        BytesRef otherResource = otelSeries("host-2", "service-a", "op-1").buildTsid(layout);

        assertThat(sameResource1, not(equalTo(sameResource2)));
        assertThat(slice(sameResource1, 0, 3), equalTo(slice(sameResource2, 0, 3)));
        assertThat(slice(sameResource1, 0, 1), equalTo(slice(otherResource, 0, 1)));
        assertThat(slice(sameResource1, 1, 3), not(equalTo(slice(otherResource, 1, 3))));
        // if these change, we'll need a new index version
        // because it means existing time series will get a new _tsid and will be routed to a different shard
        assertThat(hex(sameResource1), equalTo("e31226f31474d47326e3601d8140f616"));
        assertThat(hex(sameResource2), equalTo("e31226a4babbd20d10c85e644081c888"));
        assertThat(hex(otherResource), equalTo("e377535ed133c1fc0d6e9d79c5debd99"));
    }

    /** Bytes 1-2 depend on the prefixed dimensions only, and on both their names and values. */
    public void testPrefixedDimensionsHashCoversNamesAndValuesUnderThePrefixOnly() {
        TsidLayout layout = otelLayout(randomPrefixedDimensionsHashVersion());
        BytesRef base = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host.name", "host-1")
            .addStringDimension("attributes.operation", "op-1")
            .buildTsid(layout);
        BytesRef otherOperation = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host.name", "host-1")
            .addStringDimension("attributes.operation", "op-2")
            .buildTsid(layout);
        BytesRef otherValue = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host.name", "host-2")
            .addStringDimension("attributes.operation", "op-1")
            .buildTsid(layout);
        BytesRef otherName = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host.id", "host-1")
            .addStringDimension("attributes.operation", "op-1")
            .buildTsid(layout);
        BytesRef otherType = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host.name", "host-1")
            .addStringDimension("resource.attributes.host.port", "80")
            .addStringDimension("attributes.operation", "op-1")
            .buildTsid(layout);
        BytesRef otherTypeInt = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host.name", "host-1")
            .addIntDimension("resource.attributes.host.port", 80)
            .addStringDimension("attributes.operation", "op-1")
            .buildTsid(layout);
        assertThat(slice(base, 1, 3), equalTo(slice(otherOperation, 1, 3)));
        assertThat(slice(base, 1, 3), not(equalTo(slice(otherValue, 1, 3))));
        assertThat(slice(base, 1, 3), not(equalTo(slice(otherName, 1, 3))));
        assertThat(slice(base, 1, 3), not(equalTo(slice(otherType, 1, 3))));
        assertThat(slice(otherType, 1, 3), not(equalTo(slice(otherTypeInt, 1, 3))));
    }

    /** Values of an array-valued prefixed dimension are hashed in source order, as in the full hash. */
    public void testPrefixedDimensionsHashKeepsArrayOrder() {
        TsidLayout layout = otelLayout(randomPrefixedDimensionsHashVersion());
        BytesRef ab = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.tags", "a")
            .addStringDimension("resource.attributes.tags", "b")
            .addStringDimension("attributes.operation", "op")
            .buildTsid(layout);
        BytesRef ba = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.tags", "b")
            .addStringDimension("resource.attributes.tags", "a")
            .addStringDimension("attributes.operation", "op")
            .buildTsid(layout);
        assertThat(slice(ab, 1, 3), not(equalTo(slice(ba, 1, 3))));
    }

    /** Without a matching dimension, bytes 1-2 keep the full hash, so the tsid is the one of the previous layout. */
    public void testPrefixedDimensionsHashWithoutMatchingDimensionKeepsFullHash() {
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension(TsidBuilder.OTEL_METRIC_FIELD, "random1")
            .addStringDimension("attributes.operation", "op-1")
            .addStringDimension("resource_attributes.host", "not under the prefix")
            .addStringDimension("resource.attribute", "not under the prefix either");
        assertThat(
            builder.buildTsid(otelLayout(randomPrefixedDimensionsHashVersion())),
            equalTo(builder.buildTsid(randomSinglePrefixByteOnlyVersion()))
        );
    }

    /** A pattern is a literal path with wildcards: {@code resource.attributes.*} does not match {@code resource.attributes_extra}. */
    public void testPrefixedDimensionPatternsAreLiteral() {
        IndexVersion version = randomPrefixedDimensionsHashVersion();
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host", "h")
            .addStringDimension("resource.attributes_extra", "x")
            .addStringDimension("attributes.operation", "op-1");
        BytesRef withDot = builder.buildTsid(new TsidLayout(version, List.of("resource.attributes.*")));
        BytesRef withoutDot = builder.buildTsid(new TsidLayout(version, List.of("resource.attributes*")));
        assertThat(slice(withDot, 1, 3), not(equalTo(slice(withoutDot, 1, 3))));
        assertThat(slice(withDot, 3, 16), equalTo(slice(withoutDot, 3, 16)));
        assertTrue(TsidBuilder.isPrefixedDimension("resource.attributes_extra", path -> path.startsWith("resource.attributes")));
        assertFalse(TsidBuilder.isPrefixedDimension("resource.attributes.host", null));
    }

    /**
     * Exact paths, as the Prometheus templates use for {@code labels.job} and {@code labels.instance}: only those dimensions
     * are hashed, so series of one scrape target cluster regardless of their other labels.
     */
    public void testExactPrefixedDimensions() {
        TsidLayout layout = new TsidLayout(randomPrefixedDimensionsHashVersion(), List.of("labels.job", "labels.instance"));
        BytesRef base = prometheusSeries("api", "host-1:9100", "GET").buildTsid(layout);
        BytesRef otherLabel = prometheusSeries("api", "host-1:9100", "POST").buildTsid(layout);
        BytesRef otherInstance = prometheusSeries("api", "host-2:9100", "GET").buildTsid(layout);
        BytesRef otherJob = prometheusSeries("db", "host-1:9100", "GET").buildTsid(layout);
        assertThat(slice(base, 0, 3), equalTo(slice(otherLabel, 0, 3)));
        assertThat(slice(base, 3, 16), not(equalTo(slice(otherLabel, 3, 16))));
        assertThat(slice(base, 1, 3), not(equalTo(slice(otherInstance, 1, 3))));
        assertThat(slice(base, 1, 3), not(equalTo(slice(otherJob, 1, 3))));
        // An exact path does not match longer paths under it, nor shorter ones.
        BytesRef prefixedOnly = TsidBuilder.newBuilder()
            .addStringDimension(TsidBuilder.PROMETHEUS_LABEL_FIELD, "up")
            .addStringDimension("labels.job_name", "api")
            .addStringDimension("labels.instance.id", "host-1:9100")
            .addStringDimension("labels", "x")
            .buildTsid(layout);
        BytesRef prefixedOnlyDisabled = TsidBuilder.newBuilder()
            .addStringDimension(TsidBuilder.PROMETHEUS_LABEL_FIELD, "up")
            .addStringDimension("labels.job_name", "api")
            .addStringDimension("labels.instance.id", "host-1:9100")
            .addStringDimension("labels", "x")
            .buildTsid(new TsidLayout(layout.indexVersion(), List.of()));
        assertThat(prefixedOnly, equalTo(prefixedOnlyDisabled));
    }

    /** By default, and with an empty list, the prefixed-dimensions hash is off and the tsid is that of the previous layout. */
    public void testNoPrefixedDimensionsByDefault() {
        TsidBuilder builder = otelSeries("host-1", "service-a", "op-1");
        IndexVersion version = randomPrefixedDimensionsHashVersion();
        TsidLayout disabled = new TsidLayout(version, List.of());
        assertNull(disabled.prefixedDimensionMatcher());
        assertThat(TsidLayout.forVersion(version), equalTo(disabled));
        assertThat(builder.buildTsid(disabled), equalTo(builder.buildTsid(randomSinglePrefixByteOnlyVersion())));
        assertThat(builder.buildTsid(version), equalTo(builder.buildTsid(randomSinglePrefixByteOnlyVersion())));
        assertThat(builder.buildTsid(disabled), not(equalTo(builder.buildTsid(otelLayout(version)))));
    }

    /** Other patterns select other dimensions than the OTel ones; the rest of the tsid is unaffected. */
    public void testCustomPrefixedDimensions() {
        IndexVersion version = randomPrefixedDimensionsHashVersion();
        TsidBuilder builder = otelSeries("host-1", "service-a", "op-1");
        BytesRef otel = builder.buildTsid(otelLayout(version));
        TsidLayout custom = new TsidLayout(version, List.of("attributes.*"));
        BytesRef customPrefix = builder.buildTsid(custom);
        assertThat(slice(otel, 0, 1), equalTo(slice(customPrefix, 0, 1)));
        assertThat(slice(otel, 1, 3), not(equalTo(slice(customPrefix, 1, 3))));
        assertThat(slice(otel, 3, 16), equalTo(slice(customPrefix, 3, 16)));
        // Under the custom patterns, series sharing attributes.* cluster regardless of their resource.
        BytesRef otherResource = otelSeries("host-2", "service-b", "op-1").buildTsid(custom);
        assertThat(slice(customPrefix, 0, 3), equalTo(slice(otherResource, 0, 3)));
        // Listing the same dimensions as exact paths gives the same tsid as the wildcard pattern.
        TsidLayout exact = new TsidLayout(version, List.of("resource.attributes.service.name", "resource.attributes.host.name"));
        assertThat(builder.buildTsid(exact), equalTo(otel));
    }

    /** The layouts predating the prefixed-dimensions hash ignore the setting entirely. */
    public void testPrefixedDimensionsIgnoredByEarlierLayouts() {
        TsidBuilder builder = otelSeries("host-1", "service-a", "op-1");
        for (IndexVersion version : List.of(randomMultiplePrefixBytesVersion(), randomSinglePrefixByteOnlyVersion())) {
            BytesRef expected = builder.buildTsid(version);
            assertThat(builder.buildTsid(otelLayout(version)), equalTo(expected));
            assertThat(builder.buildTsid(new TsidLayout(version, List.of("attributes.*"))), equalTo(expected));
            assertThat(builder.buildTsid(new TsidLayout(version, List.of())), equalTo(expected));
            assertNull(otelLayout(version).prefixedDimensionMatcher());
        }
    }

    /** A reused builder must produce what a fresh builder would under the prefixed-dimensions layout as well. */
    public void testReusedBuilderMatchesFreshBuilderWithPrefixedDimensions() {
        TsidLayout layout = otelLayout(randomPrefixedDimensionsHashVersion());
        TsidBuilder reused = otelSeries("host-1", "service-a", "op-1");
        BytesRef first = reused.buildTsid(layout);
        reused.reset();
        BytesRef second = addDistinctPaths(reused).buildTsid(layout);
        assertThat(second, equalTo(addDistinctPaths(TsidBuilder.newBuilder()).buildTsid(layout)));
        reused.reset();
        assertThat(otelSeries("host-1", "service-a", "op-1").addAll(reused).buildTsid(layout), equalTo(first));
    }

    public void testPrefixByteRank() {
        assertThat(TsidBuilder.prefixByteRank(TsidBuilder.OTEL_METRIC_FIELD), equalTo(0));
        assertThat(TsidBuilder.prefixByteRank(TsidBuilder.PROMETHEUS_LABEL_FIELD), equalTo(1));
        // Any ordinary path must get PREFIX_RANK_NONE, which must beat every special rank.
        assertThat(TsidBuilder.prefixByteRank(randomAlphaOfLengthBetween(1, 20)), equalTo(TsidBuilder.PREFIX_RANK_NONE));
        assertThat(TsidBuilder.PREFIX_RANK_NONE, greaterThan(1));
    }

    public void testSingleBytePrefixSpecialFieldUsesValueSimilarity() {
        long h1 = randomLong();
        long h2 = randomLong();
        MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        byte expected = TsidBuilder.similarityByte(h1, h2, scratch);
        assertThat(TsidBuilder.singleBytePrefix(0, h1, h2, null, scratch), equalTo(expected));
        assertThat(TsidBuilder.singleBytePrefix(1, h1, h2, null, scratch), equalTo(expected));
    }

    public void testSingleBytePrefixNameSimilarityPathUsesStreamHash() {
        MurmurHash3.Hash128 nameSimilarityHash = new MurmurHash3.Hash128(randomLong(), randomLong());
        MurmurHash3.Hash128 scratch = new MurmurHash3.Hash128();
        byte expected = TsidBuilder.similarityByte(nameSimilarityHash);
        assertThat(
            TsidBuilder.singleBytePrefix(TsidBuilder.PREFIX_RANK_NONE, randomLong(), randomLong(), nameSimilarityHash, scratch),
            equalTo(expected)
        );
    }

    /**
     * The columnar prep method {@code writeSingleBytePrefixTsid} must lay out the prefixed-dimensions hash exactly as the row
     * path does, given the same inputs: hashing the prefixed dimensions through the builder's own 4-words-per-dimension stream.
     */
    public void testWriteSingleBytePrefixTsidMatchesRowPath() {
        TsidLayout layout = otelLayout(randomPrefixedDimensionsHashVersion());
        TsidBuilder builder = otelSeries("host-1", "service-a", "op-1");
        BytesRef expected = builder.buildTsid(layout);

        MurmurHash3.Hash128 fullHash = builder.hash();
        MurmurHash3.Hash128 prefixedHash = TsidBuilder.newBuilder()
            .addStringDimension("resource.attributes.host.name", "host-1")
            .addStringDimension("resource.attributes.service.name", "service-a")
            .hash();
        byte prefixByte = expected.bytes[expected.offset];
        assertThat(TsidBuilder.writeSingleBytePrefixTsid(prefixByte, prefixedHash, fullHash), equalTo(expected));

        BytesRef withoutPrefixedHash = TsidBuilder.writeSingleBytePrefixTsid(prefixByte, null, fullHash);
        assertThat(withoutPrefixedHash, equalTo(builder.buildTsid(randomSinglePrefixByteOnlyVersion())));
    }

    /**
     * The prefix byte produced by {@code singleBytePrefix} with an OTel field's value hash must equal
     * the first byte of the full TSID built by the row path for the same field value, confirming that
     * the columnar prep method and {@code computeSingleBytePrefix} agree.
     */
    public void testSingleBytePrefixMatchesTsidPrefixByteForOtelField() {
        String value = randomAlphaOfLengthBetween(1, 32);
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        TsidBuilder builder = TsidBuilder.newBuilder()
            .addStringDimension(TsidBuilder.OTEL_METRIC_FIELD, value)
            .addLongDimension("other", randomLong());
        BytesRef tsid = builder.buildTsid(IndexVersions.TSID_SINGLE_PREFIX_BYTE);

        MurmurHash3.Hash128 valueHash = TsidBuilder.hashStringValue(utf8, 0, utf8.length, new MurmurHash3.Hash128());
        byte prefix = TsidBuilder.singleBytePrefix(
            TsidBuilder.prefixByteRank(TsidBuilder.OTEL_METRIC_FIELD),
            valueHash.h1,
            valueHash.h2,
            null,
            new MurmurHash3.Hash128()
        );
        assertThat(prefix, equalTo(tsid.bytes[tsid.offset]));
    }

}
