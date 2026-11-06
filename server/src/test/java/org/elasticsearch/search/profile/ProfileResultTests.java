/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.codec.profiler.CounterMeasurement;
import org.elasticsearch.codec.profiler.CounterMetric;
import org.elasticsearch.codec.profiler.Metric;
import org.elasticsearch.codec.profiler.Snapshot;
import org.elasticsearch.codec.profiler.TimerMeasurement;
import org.elasticsearch.codec.profiler.TimerMetric;
import org.elasticsearch.codec.profiler.TimingPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;

public class ProfileResultTests extends AbstractXContentSerializingTestCase<ProfileResult> {
    public static final Predicate<String> RANDOM_FIELDS_EXCLUDE_FILTER = s -> s.endsWith(ProfileResult.BREAKDOWN.getPreferredName())
        || s.endsWith(ProfileResult.DEBUG.getPreferredName());

    public static ProfileResult createTestItem(int depth) {
        String type = randomAlphaOfLengthBetween(5, 10);
        String description = randomAlphaOfLengthBetween(5, 10);
        int breakdownsSize = randomIntBetween(0, 5);
        Map<String, Long> breakdown = Maps.newMapWithExpectedSize(breakdownsSize);
        while (breakdown.size() < breakdownsSize) {
            long value = randomNonNegativeLong();
            if (randomBoolean()) {
                // also often use "small" values in tests
                value = value % 10000;
            }
            breakdown.put(randomAlphaOfLengthBetween(5, 10), value);
        }
        int debugSize = randomIntBetween(0, 5);
        Map<String, Object> debug = Maps.newMapWithExpectedSize(debugSize);
        while (debug.size() < debugSize) {
            debug.put(randomAlphaOfLength(5), randomAlphaOfLength(4));
        }
        int childrenSize = depth > 0 ? randomIntBetween(0, 1) : 0;
        List<ProfileResult> children = new ArrayList<>(childrenSize);
        for (int i = 0; i < childrenSize; i++) {
            children.add(createTestItem(depth - 1));
        }
        return new ProfileResult(type, description, breakdown, debug, randomNonNegativeLong(), children, Map.of(), Map.of(), Map.of());
    }

    @Override
    protected ProfileResult createTestInstance() {
        return createTestItem(2);
    }

    @Override
    protected ProfileResult mutateInstance(ProfileResult instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<ProfileResult> instanceReader() {
        return ProfileResult::new;
    }

    @Override
    protected ProfileResult doParseInstance(XContentParser parser) throws IOException {
        return SearchResponseUtils.parseProfileResult(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return RANDOM_FIELDS_EXCLUDE_FILTER;
    }

    public void testCodecsAndSegmentsSurviveTheWireAndRenderBesideTheDebugOutput() throws IOException {
        final ProfileResult result = new ProfileResult(
            "t",
            "d",
            Map.of("score", 1L),
            Map.of("a", "foo"),
            10L,
            List.of(),
            codec(),
            segments(),
            Map.of()
        );

        final ProfileResult copy = copyWriteable(result, writableRegistry(), ProfileResult::new);
        assertEquals(result, copy);
        assertEquals(codec(), copy.getTotals());
        assertEquals(segments(), copy.getSegments());
        assertEquals("unrelated debug output is kept", Map.of("a", "foo"), copy.getDebugInfo());

        final Map<String, Object> rendered = XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(result), true);
        assertThat(
            "the codecs and segments sit on the node beside its debug output",
            List.copyOf(rendered.keySet()),
            equalTo(List.of("type", "description", "time_in_nanos", "breakdown", "debug", "totals", "segments"))
        );
        assertThat(rendered.get("debug"), equalTo(Map.of("a", "foo")));
        assertThat(
            "the totals are keyed by codec",
            rendered.get("totals"),
            equalTo(Map.of("ColumNAR", Map.of("blocks", Map.of("read", 63))))
        );
        assertThat(
            rendered.get("segments"),
            equalTo(
                Map.of(
                    "_1",
                    Map.of("fields", Map.of("log.level", Map.of("codec", "ColumNAR", "metrics", Map.of("blocks", Map.of("read", 63)))))
                )
            )
        );
    }

    public void testANodeRenderedFromTypedCodecReadsHasTheDocumentedShapeAndSurvivesTheWire() throws IOException {
        final CounterMetric read = CounterMetric.of("ColumNAR", "blocks", "read", Metric.Unit.COUNT, "blocks fetched");
        final TimerMetric decode = TimerMetric.of("ColumNAR", "blocks", "decode_time_nanos", TimingPolicy.ADAPTIVE, "decode")
            .withCpuTime("decode_cpu_time_nanos", "decode CPU");
        final TimerMetric open = TimerMetric.of("ColumNAR", "readers", "open_time_nanos", TimingPolicy.EXACT, "open");
        final CounterMetric other = CounterMetric.of("Second", "pages", "read", Metric.Unit.COUNT, "pages");
        final CounterMetric numeric = CounterMetric.of("Numeric", "blocks", "read", Metric.Unit.COUNT, "blocks fetched");
        final CodecReads reads = new CodecReads(
            Map.of(
                "ColumNAR",
                Snapshot.of(
                    List.of(
                        new CounterMeasurement(read, 3),
                        new TimerMeasurement(decode, 90, OptionalLong.of(40), 2, 5, true),
                        new TimerMeasurement(open, 70, OptionalLong.empty(), 2, 2, false),
                        new CounterMeasurement(other, 1)
                    )
                ),
                "Numeric",
                Snapshot.of(List.of(new CounterMeasurement(numeric, 4)))
            ),
            List.of(
                new CodecReads.Detail("_3", "status", "ColumNAR", null, Map.of(), Snapshot.of(List.of(new CounterMeasurement(read, 1)))),
                new CodecReads.Detail(
                    "_1",
                    "host.name",
                    "ColumNAR",
                    null,
                    Map.of(),
                    Snapshot.of(List.of(new TimerMeasurement(open, 70, OptionalLong.empty(), 2, 2, false)))
                )
            ),
            Map.of(),
            false,
            true,
            true
        );
        final ProfileResult result = new ProfileResult(
            "t",
            "d",
            Map.of("score", 1L),
            Map.of(),
            10L,
            List.of(),
            reads.renderTotals(),
            reads.renderSegments(),
            Map.of()
        );
        assertThat(
            result.getTotals(),
            equalTo(
                Map.of(
                    "ColumNAR",
                    Map.of(
                        "Second.pages",
                        Map.of("read", 1L),
                        "blocks",
                        Map.of(
                            "decode_cpu_time_nanos",
                            Map.of("value", 40L, "unit", "ns", "estimated", true, "total_calls", 5L, "sampled_calls", 2L),
                            "decode_time_nanos",
                            Map.of("value", 90L, "unit", "ns", "estimated", true, "total_calls", 5L, "sampled_calls", 2L),
                            "read",
                            3L
                        ),
                        "readers",
                        Map.of("open_time_nanos", Map.of("value", 70L, "unit", "ns", "estimated", false, "total_calls", 2L))
                    ),
                    "Numeric",
                    Map.of("blocks", Map.of("read", 4L))
                )
            )
        );
        assertThat("codecs are ordered by name", List.copyOf(result.getTotals().keySet()), equalTo(List.of("ColumNAR", "Numeric")));
        final Map<String, Object> openTime = Map.of(
            "ColumNAR",
            Map.of("readers", Map.of("open_time_nanos", Map.of("value", 70L, "unit", "ns", "estimated", false, "total_calls", 2L)))
        );
        final Map<String, Object> blocksRead = Map.of("ColumNAR", Map.of("blocks", Map.of("read", 1L)));
        assertThat(
            result.getSegments(),
            equalTo(
                Map.of(
                    "_1",
                    Map.of("metrics", openTime, "fields", Map.of("host.name", Map.of("metrics", openTime))),
                    "_3",
                    Map.of("metrics", blocksRead, "fields", Map.of("status", Map.of("metrics", blocksRead)))
                )
            )
        );
        assertThat(List.copyOf(result.getSegments().keySet()), equalTo(List.of("_1", "_3")));
        assertEquals(result, copyWriteable(result, writableRegistry(), ProfileResult::new));
    }

    public void testANodeWithoutCodecCountsRendersNoCodecs() {
        final String json = Strings.toString(
            new ProfileResult("t", "d", Map.of("score", 1L), Map.of(), 10L, List.of(), Map.of(), Map.of(), Map.of())
        );

        assertFalse(json, json.contains("totals"));
        assertFalse(json, json.contains("segments"));
    }

    public void testANodeThatDoesNotKnowCodecsGetsTheRestOfTheProfile() throws IOException {
        final ProfileResult result = new ProfileResult(
            "t",
            "d",
            Map.of("score", 1L),
            Map.of("a", "foo"),
            10L,
            List.of(),
            codec(),
            segments(),
            Map.of()
        );
        final TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("codec_read_profiling"));
        final ProfileResult copy = copyWriteable(result, writableRegistry(), ProfileResult::new, before);
        assertEquals(Map.of(), copy.getTotals());
        assertEquals(Map.of(), copy.getSegments());
        assertEquals(Map.of("a", "foo"), copy.getDebugInfo());
        assertEquals(result.getTime(), copy.getTime());
    }

    private static Map<String, Object> codec() {
        return Map.of("ColumNAR", Map.of("blocks", Map.of("read", 63L)));
    }

    private static Map<String, Object> segments() {
        return Map.of(
            "_1",
            Map.of("fields", Map.of("log.level", Map.of("codec", "ColumNAR", "metrics", Map.of("blocks", Map.of("read", 63L)))))
        );
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> children = new ArrayList<>();
        children.add(new ProfileResult("child1", "desc1", Map.of("key1", 100L), Map.of(), 100L, List.of(), Map.of(), Map.of(), Map.of()));
        children.add(
            new ProfileResult("child2", "desc2", Map.of("key1", 123356L), Map.of(), 123356L, List.of(), Map.of(), Map.of(), Map.of())
        );
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("key1", 123456L);
        breakdown.put("stuff", 10000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("a", "foo");
        debug.put("b", "bar");
        ProfileResult result = new ProfileResult(
            "someType",
            "some description",
            breakdown,
            debug,
            223456L,
            children,
            Map.of(),
            Map.of(),
            Map.of()
        );
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "type" : "someType",
              "description" : "some description",
              "time_in_nanos" : 223456,
              "breakdown" : {
                "key1" : 123456,
                "stuff" : 10000
              },
              "debug" : {
                "a" : "foo",
                "b" : "bar"
              },
              "children" : [
                {
                  "type" : "child1",
                  "description" : "desc1",
                  "time_in_nanos" : 100,
                  "breakdown" : {
                    "key1" : 100
                  }
                },
                {
                  "type" : "child2",
                  "description" : "desc2",
                  "time_in_nanos" : 123356,
                  "breakdown" : {
                    "key1" : 123356
                  }
                }
              ]
            }""", Strings.toString(builder));

        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "type" : "someType",
              "description" : "some description",
              "time" : "223.4micros",
              "time_in_nanos" : 223456,
              "breakdown" : {
                "key1" : 123456,
                "stuff" : 10000
              },
              "debug" : {
                "a" : "foo",
                "b" : "bar"
              },
              "children" : [
                {
                  "type" : "child1",
                  "description" : "desc1",
                  "time" : "100nanos",
                  "time_in_nanos" : 100,
                  "breakdown" : {
                    "key1" : 100
                  }
                },
                {
                  "type" : "child2",
                  "description" : "desc2",
                  "time" : "123.3micros",
                  "time_in_nanos" : 123356,
                  "breakdown" : {
                    "key1" : 123356
                  }
                }
              ]
            }""", Strings.toString(builder));

        result = new ProfileResult(
            "profileName",
            "some description",
            Map.of("key1", 12345678L),
            Map.of(),
            12345678L,
            List.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "type" : "profileName",
              "description" : "some description",
              "time" : "12.3ms",
              "time_in_nanos" : 12345678,
              "breakdown" : {
                "key1" : 12345678
              }
            }""", Strings.toString(builder));

        result = new ProfileResult(
            "profileName",
            "some description",
            Map.of("key1", 1234567890L),
            Map.of(),
            1234567890L,
            List.of(),
            Map.of(),
            Map.of(),
            Map.of()
        );
        builder = XContentFactory.jsonBuilder().prettyPrint().humanReadable(true);
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("""
            {
              "type" : "profileName",
              "description" : "some description",
              "time" : "1.2s",
              "time_in_nanos" : 1234567890,
              "breakdown" : {
                "key1" : 1234567890
              }
            }""", Strings.toString(builder));
    }
}
