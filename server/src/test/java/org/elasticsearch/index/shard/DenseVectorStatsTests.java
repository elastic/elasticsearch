/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.SegmentCalibrationParameters;
import org.elasticsearch.index.shard.DenseVectorStats.AutoCalibrationEntry;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.shard.DenseVectorStats.INCLUDE_AUTO_CALIBRATION;
import static org.elasticsearch.index.shard.DenseVectorStats.INCLUDE_OFF_HEAP;
import static org.elasticsearch.index.shard.DenseVectorStats.INCLUDE_PER_FIELD_STATS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DenseVectorStatsTests extends AbstractWireSerializingTestCase<DenseVectorStats> {
    @Override
    protected Writeable.Reader<DenseVectorStats> instanceReader() {
        return DenseVectorStats::new;
    }

    @Override
    protected DenseVectorStats createTestInstance() {
        if (randomBoolean()) {
            return new DenseVectorStats(randomNonNegativeLong(), randomOffHeap(), randomBoolean() ? randomCalibrationStats() : null);
        } else {
            return new DenseVectorStats(randomNonNegativeLong());
        }
    }

    @Override
    protected DenseVectorStats mutateInstance(DenseVectorStats instance) {
        return new DenseVectorStats(randomValueOtherThan(instance.getValueCount(), ESTestCase::randomNonNegativeLong));
    }

    Map<String, Map<String, Long>> randomOffHeap() {
        return randomMap(1, 5, () -> new Tuple<>(randomAlphaOfLength(3), randomOffHeapEntry()));
    }

    Map<String, Long> randomOffHeapEntry() {
        return randomMap(1, 5, () -> new Tuple<>(randomAlphaOfLength(3), randomNonNegativeLong()));
    }

    Map<String, List<AutoCalibrationEntry>> randomCalibrationStats() {
        return randomMap(1, 3, () -> new Tuple<>(randomAlphaOfLength(5), randomCalibrationEntries()));
    }

    List<AutoCalibrationEntry> randomCalibrationEntries() {
        int count = randomIntBetween(1, 3);
        List<AutoCalibrationEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            SegmentCalibrationParameters params = rarely() ? null : new SegmentCalibrationParameters.Osq(
                randomFrom(QuantEncoding.values()),
                randomBoolean(),
                (float) randomDoubleBetween(1.0, 3.0, true)
            );
            entries.add(new AutoCalibrationEntry(params, randomNonNegativeLong(), randomNonNegativeLong(), randomIntBetween(1, 10)));
        }
        return entries;
    }

    public void testBasicEquality() {
        DenseVectorStats stats1 = new DenseVectorStats(5L, null);
        DenseVectorStats stats2 = new DenseVectorStats(5L, null);
        assertEquals(stats1, stats2);
        stats1 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L)));
        stats2 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L)));
        assertEquals(stats1, stats2);
        stats1 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "bar", Map.of("veb", 3L)));
        stats2 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "bar", Map.of("veb", 3L)));
        assertEquals(stats1, stats2);

        stats1 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L)));
        stats2 = new DenseVectorStats(6L, Map.of("foo", Map.of("vec", 9L)));
        assertNotEquals(stats1, stats2);
        stats1 = new DenseVectorStats(6L, Map.of("foo", Map.of("vec", 8L)));
        stats2 = new DenseVectorStats(6L, Map.of("foo", Map.of("vec", 9L)));
        assertNotEquals(stats1, stats2);
        stats1 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L)));
        stats2 = new DenseVectorStats(5L, Map.of("foo", Map.of("vex", 9L)));
        assertNotEquals(stats1, stats2);
        stats1 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "bar", Map.of("veb", 3L)));
        stats2 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "bar", Map.of("veb", 2L)));
        assertNotEquals(stats1, stats2);
        stats1 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "bar", Map.of("veb", 3L)));
        stats2 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "baz", Map.of("veb", 3L)));
        assertNotEquals(stats1, stats2);
        stats1 = new DenseVectorStats(5L, null);
        stats2 = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "baz", Map.of("veb", 3L)));
        assertNotEquals(stats1, stats2);
        assertNotEquals(stats2, stats1);
    }

    public void testBasicXContent() throws IOException {
        var stats = new DenseVectorStats(
            5L,
            Map.of("foo", Map.of("vec", 9L), "bar", Map.of("vec", 14L, "vex", 1L, "veb", 3L, "cenivf", 7L, "clivf", 2L))
        );

        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String expected = """
            {
              "dense_vector" : {
                "value_count" : 5
              }
            }""";
        assertThat(Strings.toString(builder), equalTo(expected));

        builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true")));
        builder.endObject();
        expected = """
            {
              "dense_vector" : {
                "value_count" : 5,
                "off_heap" : {
                  "total_size_bytes" : 36,
                  "total_veb_size_bytes" : 3,
                  "total_vec_size_bytes" : 23,
                  "total_veq_size_bytes" : 0,
                  "total_vex_size_bytes" : 1,
                  "total_cenivf_size_bytes" : 7,
                  "total_clivf_size_bytes" : 2
                }
              }
            }""";
        assertThat(Strings.toString(builder), equalTo(expected));

        builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        stats.toXContent(builder, new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true")));
        builder.endObject();
        expected = """
            {
              "dense_vector" : {
                "value_count" : 5,
                "off_heap" : {
                  "total_size_bytes" : 36,
                  "total_veb_size_bytes" : 3,
                  "total_vec_size_bytes" : 23,
                  "total_veq_size_bytes" : 0,
                  "total_vex_size_bytes" : 1,
                  "total_cenivf_size_bytes" : 7,
                  "total_clivf_size_bytes" : 2,
                  "fielddata" : {
                    "bar" : {
                      "cenivf_size_bytes" : 7,
                      "clivf_size_bytes" : 2,
                      "veb_size_bytes" : 3,
                      "vec_size_bytes" : 14,
                      "vex_size_bytes" : 1
                    },
                    "foo" : {
                      "vec_size_bytes" : 9
                    }
                  }
                }
              }
            }""";
        assertThat(Strings.toString(builder), equalTo(expected));

        for (var s : List.of(new DenseVectorStats(11L), new DenseVectorStats(11L, Map.of()))) {
            var paramOptions = List.of(
                new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true")),
                new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true"))
            );
            for (var params : paramOptions) {
                builder = XContentFactory.jsonBuilder().prettyPrint();
                builder.startObject();
                s.toXContent(builder, params);
                builder.endObject();
                expected = """
                    {
                      "dense_vector" : {
                        "value_count" : 11,
                        "off_heap" : {
                          "total_size_bytes" : 0,
                          "total_veb_size_bytes" : 0,
                          "total_vec_size_bytes" : 0,
                          "total_veq_size_bytes" : 0,
                          "total_vex_size_bytes" : 0,
                          "total_cenivf_size_bytes" : 0,
                          "total_clivf_size_bytes" : 0
                        }
                      }
                    }""";
                assertThat(Strings.toString(builder), equalTo(expected));
            }
        }
    }

    public void testXContentHumanReadable() throws IOException {
        var bar = Map.of("vec", 4194304L, "vex", 100000000L, "veb", 1024L);
        var baz = Map.of("vec", 2097152L, "vex", 100000000L, "veb", 2048L);
        var foo = Map.of("vec", 1048576L, "veq", 1099511627776L);
        var stats = new DenseVectorStats(5678L, Map.of("foo", foo, "bar", bar, "baz", baz));

        var builder = XContentFactory.jsonBuilder().humanReadable(true).prettyPrint();
        builder.startObject();
        stats.toXContent(builder, new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true")));
        builder.endObject();
        String expected = """
            {
              "dense_vector" : {
                "value_count" : 5678,
                "off_heap" : {
                  "total_size" : "1tb",
                  "total_size_bytes" : 1099718970880,
                  "total_veb_size" : "3kb",
                  "total_veb_size_bytes" : 3072,
                  "total_vec_size" : "7mb",
                  "total_vec_size_bytes" : 7340032,
                  "total_veq_size" : "1tb",
                  "total_veq_size_bytes" : 1099511627776,
                  "total_vex_size" : "190.7mb",
                  "total_vex_size_bytes" : 200000000,
                  "total_cenivf_size" : "0b",
                  "total_cenivf_size_bytes" : 0,
                  "total_clivf_size" : "0b",
                  "total_clivf_size_bytes" : 0,
                  "fielddata" : {
                    "bar" : {
                      "veb_size" : "1kb",
                      "veb_size_bytes" : 1024,
                      "vec_size" : "4mb",
                      "vec_size_bytes" : 4194304,
                      "vex_size" : "95.3mb",
                      "vex_size_bytes" : 100000000
                    },
                    "baz" : {
                      "veb_size" : "2kb",
                      "veb_size_bytes" : 2048,
                      "vec_size" : "2mb",
                      "vec_size_bytes" : 2097152,
                      "vex_size" : "95.3mb",
                      "vex_size_bytes" : 100000000
                    },
                    "foo" : {
                      "vec_size" : "1mb",
                      "vec_size_bytes" : 1048576,
                      "veq_size" : "1tb",
                      "veq_size_bytes" : 1099511627776
                    }
                  }
                }
              }
            }""";
        assertThat(Strings.toString(builder), equalTo(expected));
    }

    public void testAutoCalibrationXContent() throws IOException {
        var calibEntry = new AutoCalibrationEntry(
            new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, true, 2.0f),
            100000L,
            10000000L,
            2
        );
        var stats = new DenseVectorStats(100000L, Map.of("my_vec", Map.of("vec", 10000000L)), Map.of("my_vec", List.of(calibEntry)));

        // without include_auto_calibration: no auto_calibration block
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true")));
        builder.endObject();
        assertFalse("auto_calibration should not appear without param", Strings.toString(builder).contains("auto_calibration"));

        // with include_auto_calibration=true
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(
            builder,
            new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true", INCLUDE_AUTO_CALIBRATION, "true"))
        );
        builder.endObject();
        String output = Strings.toString(builder);
        assertThat(output, containsString("\"calibrated\":true"));
        assertThat(output, containsString("\"bits\":1"));
        assertThat(output, containsString("\"query_bits\":4"));
        assertThat(output, containsString("\"precondition\":true"));
        assertThat(output, containsString("\"oversample\":2.0"));
        assertThat(output, containsString("\"number_of_vectors\":100000"));
        assertThat(output, containsString("\"size_in_bytes\":10000000"));
        assertThat(output, containsString("\"number_of_segments\":2"));
    }

    public void testAutoCalibrationXContentHumanReadable() throws IOException {
        var calibEntry = new AutoCalibrationEntry(
            new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, false, 1.5f),
            50000L,
            4194304L,
            1
        );

        var stats = new DenseVectorStats(50000L, Map.of("vec_field", Map.of("vec", 4194304L)), Map.of("vec_field", List.of(calibEntry)));

        XContentBuilder builder = XContentFactory.jsonBuilder().humanReadable(true).prettyPrint();
        builder.startObject();
        stats.toXContent(
            builder,
            new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true", INCLUDE_AUTO_CALIBRATION, "true"))
        );
        builder.endObject();
        String output = Strings.toString(builder);
        assertTrue("size human-readable should appear", output.contains("\"size\" : \"4mb\""));
        assertTrue("size_in_bytes should appear", output.contains("\"size_in_bytes\" : 4194304"));
    }

    public void testAutoCalibrationNotShownWithoutParam() throws IOException {
        var calibEntry = new AutoCalibrationEntry(
            new SegmentCalibrationParameters.Osq(QuantEncoding.FOUR_BIT_SYMMETRIC, false, 2.5f),
            20000L,
            1000000L,
            1
        );
        var stats = new DenseVectorStats(20000L, Map.of("field", Map.of("vec", 1000000L)), Map.of("field", List.of(calibEntry)));

        // include_auto_calibration absent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true")));
        builder.endObject();
        assertFalse(Strings.toString(builder).contains("auto_calibration"));

        // include_auto_calibration=false
        builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(
            builder,
            new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true", INCLUDE_AUTO_CALIBRATION, "false"))
        );
        builder.endObject();
        assertFalse(Strings.toString(builder).contains("auto_calibration"));
    }

    public void testBasicAdd() {
        DenseVectorStats stats1 = new DenseVectorStats(5L);
        DenseVectorStats stats2 = new DenseVectorStats(6L);
        stats1.add(stats2);
        assertEquals(new DenseVectorStats(11L), stats1);

        stats1 = new DenseVectorStats(8L, Map.of("foo", Map.of("vec", 9L)));
        stats2 = new DenseVectorStats(2L);
        stats1.add(stats2);
        assertEquals(new DenseVectorStats(10L, Map.of("foo", Map.of("vec", 9L))), stats1);

        stats1 = new DenseVectorStats(3L);
        stats2 = new DenseVectorStats(9L, Map.of("foo", Map.of("vec", 11L)));
        stats1.add(stats2);
        assertEquals(new DenseVectorStats(12L, Map.of("foo", Map.of("vec", 11L))), stats1);

        stats1 = new DenseVectorStats(1L, Map.of("bar", Map.of("vex", 13L)));
        stats2 = new DenseVectorStats(1L, Map.of("foo", Map.of("vex", 14L)));
        stats1.add(stats2);
        assertEquals(new DenseVectorStats(2L, Map.of("foo", Map.of("vex", 14L), "bar", Map.of("vex", 13L))), stats1);

        stats1 = new DenseVectorStats(1L, Map.of("bar", Map.of("vex", 13L)));
        stats2 = new DenseVectorStats(1L, Map.of("foo", Map.of("vec", 14L)));
        stats1.add(stats2);
        assertEquals(new DenseVectorStats(2L, Map.of("foo", Map.of("vec", 14L), "bar", Map.of("vex", 13L))), stats1);

        stats1 = new DenseVectorStats(1L, Map.of("bar", Map.of("vex", 11L)));
        stats2 = new DenseVectorStats(1L, Map.of("bar", Map.of("vex", 13L)));
        stats1.add(stats2);
        assertEquals(new DenseVectorStats(2L, Map.of("bar", Map.of("vex", 24L))), stats1);

        stats1 = new DenseVectorStats(1L, Map.of("bar", Map.of("vex", 11L, "vec", 6L)));
        stats2 = new DenseVectorStats(1L, Map.of("bar", Map.of("vex", 13L, "veb", 7L)));
        stats1.add(stats2);
        assertEquals(new DenseVectorStats(2L, Map.of("bar", Map.of("veb", 7L, "vec", 6L, "vex", 24L))), stats1);
    }

    public void testAutoCalibrationAddSameParams() {
        var params = new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, true, 2.0f);
        var stats1 = new DenseVectorStats(10000L, Map.of("f", Map.of("vec", 1000000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(params, 10000L, 1000000L, 1)
        )));
        var stats2 = new DenseVectorStats(20000L, Map.of("f", Map.of("vec", 2000000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(params, 20000L, 2000000L, 1)
        )));
        stats1.add(stats2);

        List<AutoCalibrationEntry> merged = stats1.calibrationStats().get("f");
        assertEquals(1, merged.size());
        assertEquals(30000L, merged.get(0).numberOfVectors);
        assertEquals(3000000L, merged.get(0).sizeInBytes);
        assertEquals(2, merged.get(0).numberOfSegments);
    }

    public void testAutoCalibrationAddDifferentParams() {
        var params1bit = new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, true, 2.0f);
        var params4bit = new SegmentCalibrationParameters.Osq(QuantEncoding.FOUR_BIT_SYMMETRIC, false, 1.5f);
        var stats1 = new DenseVectorStats(10000L, Map.of("f", Map.of("vec", 1000000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(params1bit, 10000L, 1000000L, 1)
        )));
        var stats2 = new DenseVectorStats(5000L, Map.of("f", Map.of("vec", 500000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(params4bit, 5000L, 500000L, 1)
        )));
        stats1.add(stats2);

        assertEquals(2, stats1.calibrationStats().get("f").size());
    }

    public void testAutoCalibrationAddSameEncodingDifferentPrecondition() {
        var paramsWithPrecond = new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, true, 2.0f);
        var paramsNoPrecond = new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, false, 2.0f);
        var stats1 = new DenseVectorStats(10000L, Map.of("f", Map.of("vec", 1000000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(paramsWithPrecond, 10000L, 1000000L, 1)
        )));
        var stats2 = new DenseVectorStats(5000L, Map.of("f", Map.of("vec", 500000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(paramsNoPrecond, 5000L, 500000L, 1)
        )));
        stats1.add(stats2);

        assertEquals(2, stats1.calibrationStats().get("f").size());
    }

    public void testAutoCalibrationAddNullCalibrationStats() {
        var params = new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, true, 2.0f);
        var entry = new AutoCalibrationEntry(params, 10000L, 1000000L, 1);

        var noCalib = new DenseVectorStats(5L);
        var withCalib = new DenseVectorStats(5L, Map.of("k", Map.of("vec", 1L)), Map.of("k", List.of(entry)));
        noCalib.add(withCalib);
        assertNotNull(noCalib.calibrationStats());
        assertEquals(1, noCalib.calibrationStats().get("k").size());

        var withCalib2 = new DenseVectorStats(5L, Map.of("k", Map.of("vec", 1L)), Map.of("k", List.of(entry)));
        withCalib2.add(new DenseVectorStats(5L));
        assertNotNull(withCalib2.calibrationStats());
        assertEquals(1, withCalib2.calibrationStats().get("k").size());
    }

    public void testAutoCalibrationAddUncalibrated() {
        var stats1 = new DenseVectorStats(5000L, Map.of("f", Map.of("vec", 500000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(null, 5000L, 500000L, 2)
        )));
        var stats2 = new DenseVectorStats(3000L, Map.of("f", Map.of("vec", 300000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(null, 3000L, 300000L, 1)
        )));
        stats1.add(stats2);

        List<AutoCalibrationEntry> merged = stats1.calibrationStats().get("f");
        assertEquals(1, merged.size());
        assertNull(merged.get(0).parameters);
        assertEquals(8000L, merged.get(0).numberOfVectors);
        assertEquals(800000L, merged.get(0).sizeInBytes);
        assertEquals(3, merged.get(0).numberOfSegments);
    }

    public void testUncalibratedXContent() throws IOException {
        var stats = new DenseVectorStats(5000L, Map.of("f", Map.of("vec", 500000L)), Map.of("f", List.of(
            new AutoCalibrationEntry(null, 5000L, 500000L, 3)
        )));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(
            builder,
            new ToXContent.MapParams(Map.of(INCLUDE_OFF_HEAP, "true", INCLUDE_PER_FIELD_STATS, "true", INCLUDE_AUTO_CALIBRATION, "true"))
        );
        builder.endObject();
        String output = Strings.toString(builder);
        assertThat(output, containsString("\"calibrated\":false"));
        assertThat(output, containsString("\"number_of_vectors\":5000"));
        assertThat(output, containsString("\"size_in_bytes\":500000"));
        assertThat(output, containsString("\"number_of_segments\":3"));
        assertFalse("parameters block should not appear for uncalibrated entry", output.contains("\"parameters\""));
    }

    public void testUncalibratedEntrySerialization() throws IOException {
        var entry = new AutoCalibrationEntry(null, 1234L, 56789L, 2);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            entry.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                var deserialized = new AutoCalibrationEntry(in);
                assertNull(deserialized.parameters);
                assertEquals(1234L, deserialized.numberOfVectors);
                assertEquals(56789L, deserialized.sizeInBytes);
                assertEquals(2, deserialized.numberOfSegments);
            }
        }
    }

    public void testSerializationOldTransportVersionOmitsCalibrationStats() throws IOException {
        var params = new SegmentCalibrationParameters.Osq(QuantEncoding.ONE_BIT_4BIT_QUERY, true, 2.0f);
        var entry = new AutoCalibrationEntry(params, 10000L, 1000000L, 1);
        var stats = new DenseVectorStats(10000L, Map.of("f", Map.of("vec", 1000000L)), Map.of("f", List.of(entry)));

        TransportVersion oldVersion = TransportVersionUtils.randomVersionNotSupporting(
            DenseVectorStats.DENSE_VECTOR_AUTO_CALIBRATION_STATS
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            stats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(oldVersion);
                DenseVectorStats deserialized = new DenseVectorStats(in);
                assertNull("calibration stats should be absent for old transport versions", deserialized.calibrationStats());
            }
        }
    }
}
