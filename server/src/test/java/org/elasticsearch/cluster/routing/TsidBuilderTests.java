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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class TsidBuilderTests extends ESTestCase {

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
        assertThat(builder.hash().toString(), equalTo("0xd4de1356065d297a2be489781e15d256")); // used to make shard routing decisions
        BytesRef bytesRef = builder.buildTsid();
        assertThat(bytesRef, notNullValue());
        // 4 bytes for path hash + 1 byte per value (up to 16, only first value for arrays) + 16 bytes for hash
        assertThat(bytesRef.length, equalTo(26));
        assertThat(
            HexFormat.of().formatHex(bytesRef.bytes, bytesRef.offset, bytesRef.length),
            equalTo("bf438ddaa0a8d663fdbb56d2151e7889e42b7a295d065613ded4") // _tsid in hex format
        );
    }

    public void testOrderingOfDifferentFieldsDoesNotMatter() {
        assertEqualBuilders(
            TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("baz", "qux"),
            TsidBuilder.newBuilder().addStringDimension("baz", "qux").addStringDimension("foo", "bar")
        );
    }

    public void testOrderingOfMultiFieldsMatters() {
        assertThat(
            Set.of(
                TsidBuilder.newBuilder().addStringDimension("foo", "bar").addStringDimension("foo", "baz").buildTsid(),
                TsidBuilder.newBuilder().addStringDimension("foo", "baz").addStringDimension("foo", "bar").buildTsid()
            ),
            hasSize(2)
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
        assertThat(Arrays.stream(tsidBuilders).map(TsidBuilder::buildTsid).distinct().toList(), hasSize(1));
        assertThat(Arrays.stream(tsidBuilders).map(TsidBuilder::hash).distinct().toList(), hasSize(1));
        assertThat(tsidBuilders[0].buildTsid(), notNullValue());
        assertThat(tsidBuilders[0].buildTsid().length, greaterThan(0));
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
        // Test that exception is thrown when no dimensions are added
        TsidBuilder builder = TsidBuilder.newBuilder();

        IllegalArgumentException hashException = expectThrows(IllegalArgumentException.class, builder::hash);
        assertTrue(hashException.getMessage().contains("Dimensions are empty"));

        IllegalArgumentException tsidException = expectThrows(IllegalArgumentException.class, builder::buildTsid);
        assertTrue(tsidException.getMessage().contains("Dimensions are empty"));
    }

    public void testTsidMinSize() {
        BytesRef tsid = TsidBuilder.newBuilder().addIntDimension("test_int", 42).buildTsid();

        // The TSID format should be: 4 bytes for path hash + 1 byte per value (up to 16) + 16 bytes for hash
        // Since we only added one dimension, we expect: 4 + 1 + 16 = 21 bytes
        assertEquals(21, tsid.length);
    }

    public void testTsidMaxSize() {
        TsidBuilder tsidBuilder = TsidBuilder.newBuilder();
        for (int i = 0; i < 32; i++) {
            tsidBuilder.addStringDimension("dimension_" + i, "value_" + i);
        }

        // The TSID format should be: 4 bytes for path hash + 1 byte per value (up to 16) + 16 bytes for hash
        // Since we added 32 dimensions, we expect: 4 + 16 + 16 = 36 bytes
        assertEquals(36, tsidBuilder.buildTsid().length);
    }
}
