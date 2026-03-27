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
        BytesRef newTsid = builder.buildTsid(randomSinglePrefixByteVersion());
        if (TsidBuilder.SINGLE_PREFIX_BYTE_ENABLED) {
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
        if (TsidBuilder.SINGLE_PREFIX_BYTE_ENABLED) {
            assertThat(builder.buildTsid(randomSinglePrefixByteVersion()).length, equalTo(16));
        } else {
            assertThat(builder.buildTsid(randomSinglePrefixByteVersion()).length, equalTo(19));
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
        BytesRef newTsid = builder.buildTsid(randomSinglePrefixByteVersion());
        assertThat(
            HexFormat.of().formatHex(oldTsid.bytes, oldTsid.offset, oldTsid.length),
            equalTo("01e3a0a8d693dbccd8eed09bb80b82b55d9756c7a6")
        );
        if (TsidBuilder.SINGLE_PREFIX_BYTE_ENABLED) {
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
        BytesRef newTsid = builder.buildTsid(randomSinglePrefixByteVersion());
        if (TsidBuilder.SINGLE_PREFIX_BYTE_ENABLED) {
            assertThat(
                HexFormat.of().formatHex(newTsid.bytes, newTsid.offset, newTsid.length),
                equalTo("e321311bea0fc3c9cbd40c8047c484aa")
            );
        } else {
            assertThat(newTsid, equalTo(oldTsid));
        }
    }
}
