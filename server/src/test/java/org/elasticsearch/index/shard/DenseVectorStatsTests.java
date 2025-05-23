/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.shard.DenseVectorStats.INCLUDE_OFF_HEAP;
import static org.elasticsearch.index.shard.DenseVectorStats.INCLUDE_PER_FIELD_STATS;
import static org.hamcrest.Matchers.equalTo;

public class DenseVectorStatsTests extends AbstractWireSerializingTestCase<DenseVectorStats> {
    @Override
    protected Writeable.Reader<DenseVectorStats> instanceReader() {
        return DenseVectorStats::new;
    }

    @Override
    protected DenseVectorStats createTestInstance() {
        if (randomBoolean()) {
            return new DenseVectorStats(randomNonNegativeLong(), randomOffHeap());
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
        var stats = new DenseVectorStats(5L, Map.of("foo", Map.of("vec", 9L), "bar", Map.of("vec", 14L, "vex", 1L, "veb", 3L)));

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
                  "total_size_bytes" : 27,
                  "total_veb_size_bytes" : 3,
                  "total_vec_size_bytes" : 23,
                  "total_veq_size_bytes" : 0,
                  "total_vex_size_bytes" : 1
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
                  "total_size_bytes" : 27,
                  "total_veb_size_bytes" : 3,
                  "total_vec_size_bytes" : 23,
                  "total_veq_size_bytes" : 0,
                  "total_vex_size_bytes" : 1,
                  "fielddata" : {
                    "bar" : {
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
                          "total_vex_size_bytes" : 0
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
}
