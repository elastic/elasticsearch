/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FieldStats;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class TextStructureUtilsTests extends TextStructureTestCase {
    private static final boolean ECS_COMPATIBILITY_DISABLED = false;
    private static final boolean ECS_COMPATIBILITY_ENABLED = true;

    private static final Collection<Boolean> ecsCompatibilityModes = Arrays.asList(ECS_COMPATIBILITY_ENABLED, ECS_COMPATIBILITY_DISABLED);

    public void testMoreLikelyGivenText() {
        assertTrue(TextStructureUtils.isMoreLikelyTextThanKeyword("the quick brown fox jumped over the lazy dog"));
        assertTrue(TextStructureUtils.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(257, 10000)));
    }

    public void testMoreLikelyGivenKeyword() {
        assertFalse(TextStructureUtils.isMoreLikelyTextThanKeyword("1"));
        assertFalse(TextStructureUtils.isMoreLikelyTextThanKeyword("DEBUG"));
        assertFalse(TextStructureUtils.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(1, 256)));
    }

    public void testGuessTimestampGivenSingleSampleSingleField() {
        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Collections.singletonList(sample),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndConsistentTimeFieldOverride() {

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampField("field1").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Collections.singletonList(sample),
            overrides,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndImpossibleTimeFieldOverride() {

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampField("field2").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TextStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample), overrides, NOOP_TIMEOUT_CHECKER)
        );

        assertEquals("Specified timestamp field [field2] is not present in record [{field1=2018-05-24T17:28:31,735}]", e.getMessage());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndConsistentTimeFormatOverride() {

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampFormat("ISO8601").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Collections.singletonList(sample),
            overrides,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndImpossibleTimeFormatOverride() {

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampFormat("EEE MMM dd HH:mm:ss yyyy").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TextStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample), overrides, NOOP_TIMEOUT_CHECKER)
        );

        assertEquals(
            "Specified timestamp format [EEE MMM dd HH:mm:ss yyyy] does not match for record [{field1=2018-05-24T17:28:31,735}]",
            e.getMessage()
        );
    }

    public void testGuessTimestampGivenSamplesWithSameSingleTimeField() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("field1", "2018-05-24T17:33:39,406");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenNullStringTimestampFormatOverride() {
        var overrides = TextStructureOverrides.builder().setTimestampFormat("null").build();

        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("field1", "2018-05-24T17:33:39,406");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            overrides,
            NOOP_TIMEOUT_CHECKER
        );
        assertNull(match);
    }

    public void testGuessTimestampGivenSamplesWithOneSingleTimeFieldDifferentFormat() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("field1", "Thu May 24 17:33:39 2018");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNull(match);
    }

    public void testGuessTimestampGivenSamplesWithDifferentSingleTimeField() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("another_field", "2018-05-24T17:33:39,406");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNull(match);
    }

    public void testGuessTimestampGivenSingleSampleManyFieldsOneTimeFormat() {
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("foo", "not a time");
        sample.put("time", "2018-05-24 17:28:31,735");
        sample.put("bar", 42);
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Collections.singletonList(sample),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("yyyy-MM-dd HH:mm:ss,SSS"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSamplesWithManyFieldsSameSingleTimeFormat() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time", "2018-05-24 17:28:31,735");
        sample1.put("bar", 42);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time", "2018-05-29 11:53:02,837");
        sample2.put("bar", 17);
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("yyyy-MM-dd HH:mm:ss,SSS"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSamplesWithManyFieldsSameTimeFieldDifferentTimeFormat() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time", "2018-05-24 17:28:31,735");
        sample1.put("bar", 42);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time", "May 29 2018 11:53:02");
        sample2.put("bar", 17);
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNull(match);
    }

    public void testGuessTimestampGivenSamplesWithManyFieldsSameSingleTimeFormatDistractionBefore() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("red_herring", "May 29 2007 11:53:02");
        sample1.put("time", "2018-05-24 17:28:31,735");
        sample1.put("bar", 42);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("red_herring", "whatever");
        sample2.put("time", "2018-05-29 11:53:02,837");
        sample2.put("bar", 17);
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("yyyy-MM-dd HH:mm:ss,SSS"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSamplesWithManyFieldsSameSingleTimeFormatDistractionAfter() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time", "May 24 2018 17:28:31");
        sample1.put("red_herring", "2018-05-24 17:28:31,735");
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time", "May 29 2018 11:53:02");
        sample2.put("red_herring", "17");
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("MMM dd yyyy HH:mm:ss", "MMM  d yyyy HH:mm:ss", "MMM d yyyy HH:mm:ss"));
        assertEquals("CISCOTIMESTAMP", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSamplesWithManyFieldsInconsistentTimeFields() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time1", "May 24 2018 17:28:31");
        sample1.put("bar", 17);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time2", "May 29 2018 11:53:02");
        sample2.put("bar", 42);
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNull(match);
    }

    public void testGuessTimestampGivenSamplesWithManyFieldsInconsistentAndConsistentTimeFields() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time1", "2018-05-09 17:28:31,735");
        sample1.put("time2", "May  9 2018 17:28:31");
        sample1.put("bar", 17);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time2", "May 10 2018 11:53:02");
        sample2.put("time3", "Thu, May 10 2018 11:53:02");
        sample2.put("bar", 42);
        Tuple<String, TimestampFormatFinder> match = TextStructureUtils.guessTimestampField(
            explanation,
            Arrays.asList(sample1, sample2),
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );
        assertNotNull(match);
        assertEquals("time2", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("MMM dd yyyy HH:mm:ss", "MMM  d yyyy HH:mm:ss", "MMM d yyyy HH:mm:ss"));
        assertEquals("CISCOTIMESTAMP", match.v2().getGrokPatternName());
    }

    public void testGuessMappingGivenNothing() {
        assertNull(guessMapping(explanation, "foo", Collections.emptyList()));
    }

    public void testGuessMappingGivenKeyword() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("ERROR", "INFO", "DEBUG")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "not a date")));
    }

    public void testGuessMappingGivenText() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "text");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("a", "the quick brown fox jumped over the lazy dog")));
    }

    public void testGuessMappingGivenIp() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("10.0.0.1", "172.16.0.1", "192.168.0.1")));
    }

    public void testGuessMappingGivenDouble() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "double");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("3.14159265359", "0", "-8")));
        // 12345678901234567890 is too long for long
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("1", "2", "12345678901234567890")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(3.14159265359, 0.0, 1e-308)));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("-1e-1", "-1e308", "1e-308")));
    }

    public void testGuessMappingGivenLong() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("500", "3", "-3")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(500, 6, 0)));
    }

    public void testGuessMappingGivenDate() {
        Map<String, String> expected = new HashMap<>();
        expected.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
        expected.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "2018-06-11T13:27:12Z")));

        // The special value of "null" for the timestamp format indicates that the analysis
        // of semi-structured text should assume the absence of any timestamp.
        // In the case of structured text, there may be timestamps present in multiple fields
        // which we want the analysis to identify. For now we don't want the user supplied timestamp
        // format override to affect this behaviour, hence this check.
        assertEquals(
            expected,
            guessMapping(
                explanation,
                "foo",
                Arrays.asList("2018-06-11T13:26:47Z", "2018-06-11T13:27:12Z"),
                TextStructureUtils.NULL_TIMESTAMP_FORMAT
            )
        );

    }

    public void testGuessMappingGivenBoolean() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "boolean");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("false", "true")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(true, false)));
    }

    public void testGuessMappingGivenArray() {
        Map<String, String> expectedLong = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long");
        assertEquals(expectedLong, guessMapping(explanation, "foo", Arrays.asList(42, Arrays.asList(1, -99))));

        Map<String, String> expectedKeyword = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword");
        assertEquals(expectedKeyword, guessMapping(explanation, "foo", Arrays.asList(new String[] { "x", "y" }, "z")));
    }

    public void testGuessMappingGivenObject() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "object");

        assertEquals(
            expected,
            guessMapping(
                explanation,
                "foo",
                Arrays.asList(Collections.singletonMap("name", "value1"), Collections.singletonMap("name", "value2"))
            )
        );
    }

    public void testGuessMappingGivenObjectAndNonObject() {
        RuntimeException e = expectThrows(
            RuntimeException.class,
            () -> guessMapping(explanation, "foo", Arrays.asList(Collections.singletonMap("name", "value1"), "value2"))
        );
        assertEquals("Field [foo] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    /**
     * Input:
     * {
     *  "foo": [
     *    {},
     *    {"name": "value"},
     *  ]
     * }
     */
    public void testGuessMappingGivenEmptyObjectFollowedByAnotherObject() {
        var input = Map.of("foo", Arrays.asList(Collections.emptyMap(), Collections.singletonMap("name", "value1")));

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 3);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "foo.name", "keyword");
    }

    /**
     * Input:
     * {
     *  "host": {"id": 1, "category": "NETWORKING DEVICE"},
     *  "timestamp": "1478261151445",
     *  "message": "Connection established"
     * }
     */
    public void testGuessMappingGivenNestedObjectAndNoRecursion() {
        Map<String, Object> nestedObject = new LinkedHashMap<>();
        nestedObject.put("id", 1);
        nestedObject.put("category", "NETWORKING DEVICE");

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("host", nestedObject);
        input.put("timestamp", "1478261151445");
        input.put("message", "Connection established");

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 1);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "host", "object");
        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
        assertKeyAndMappedType(mappings, "message", "keyword");
    }

    /**
     * Input:
     * {
     *  "host": {"id": 1, "category": "NETWORKING DEVICE"},
     *  "timestamp": "1478261151445",
     *  "message": "Connection established"
     * }
     */
    public void testGuessMappingRecursiveGivenNestedObject() {
        Map<String, Object> nestedObject = new LinkedHashMap<>();
        nestedObject.put("id", 1);
        nestedObject.put("category", "NETWORKING DEVICE");

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("host", nestedObject);
        input.put("timestamp", "1478261151445");
        input.put("message", "Connection established");

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "host.id", "long");
        assertKeyAndMappedType(mappings, "host.category", "keyword");
        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
        assertKeyAndMappedType(mappings, "message", "keyword");
    }

    /**
     * Input:
     * {
     *  "hosts": [
     *      {"id": 1, "name": "host1"},
     *      {"id": 1, "name": "host1"},
     *      {"id": 1, "name": "host1"}
     *  ],
     *  "timestamp": "1478261151445"
     * }
     */
    public void testGuessMappingRecursiveGivenArrayWithObjects() {
        Map<String, Object> nestedObject = new LinkedHashMap<>();
        nestedObject.put("id", 1);
        nestedObject.put("name", "host1");

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("host", List.of(nestedObject, nestedObject, nestedObject));
        input.put("timestamp", "1478261151445");

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "host.id", "long");
        assertKeyAndMappedType(mappings, "host.name", "keyword");
        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
    }

    /**
     * Input:
     * {
     *  "hosts": [
     *      [{"id": 1, "name": "host1"}],
     *      [[{"id": 1, "name": "host1"}]],
     *      [[[{"id": 1, "name": "host1"}]]]
     *  ],
     *  "timestamp": "1478261151445"
     * }
     */
    public void testGuessMappingRecursiveGivenNestedLists() {
        Map<String, Object> nestedObject = new LinkedHashMap<>();
        nestedObject.put("id", 1);
        nestedObject.put("name", "host1");

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("host", List.of(List.of(nestedObject), List.of(List.of(nestedObject)), List.of(List.of(List.of(nestedObject)))));
        input.put("timestamp", "1478261151445");

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "host.id", "long");
        assertKeyAndMappedType(mappings, "host.name", "keyword");
        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
    }

    /**
     * Input:
     * {
     *  "host": {},
     *  "message" : { "content" : {}}
     *  "timestamp": "1478261151445"
     * }
     */
    public void testGuessMappingRecursiveEmptyObjectMappedToObjectType() {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("host", Map.of());
        input.put("message", Map.of("content", Map.of()));
        input.put("timestamp", "1478261151445");

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "host", "object");
        assertKeyAndMappedType(mappings, "message.content", "object");
        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
    }

    /**
     * Input:
     * {
     *  "hosts": { "host": [4, { "id": 3}]},
     *  "timestamp": "1478261151445"
     * }
     */
    public void testGuessMappingRecursiveWithListOfObjectsAndConcreteValues() {
        var innerList = List.of(4, Map.of("id", 3));
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("hosts", Map.of("host", innerList));
        input.put("timestamp", "1478261151445");

        Exception e = expectThrows(
            RuntimeException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10)
        );
        assertEquals("Field [hosts.host] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    /**
     * Input:
     * {
     *  "key":
     *    [
     *      {"x": 1},
     *      {"y": {"z": 10}},
     *      {"y": {"z": 42}}
     *    ]
     * }
     */
    public void testGuessMappingRecursiveWithListOfDifferentObjects1() {
        var innerList = List.of(Map.of("x", 1), Map.of("y", Map.of("z", 10)), Map.of("y", Map.of("z", 42)));
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", innerList);

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "key.x", "long");
        assertKeyAndMappedType(mappings, "key.y.z", "long");
    }

    /**
     * Input:
     * {
     *   "key": [
     *     {"x": 1},
     *     {"y": {"z": 10}},
     *     {"y": {"z": "a"}}
     *   ]
     * }
     */
    public void testGuessMappingRecursiveWithListOfDifferentObjects2() {
        var innerList = List.of(Map.of("x", 1), Map.of("y", Map.of("z", 10)), Map.of("y", Map.of("z", "a")));
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", innerList);

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "key.x", "long");
        assertKeyAndMappedType(mappings, "key.y.z", "keyword");
    }

    /**
     * Input:
     * {
     *   "key": [
     *     {"x": 1},
     *     {"y": {"z": 10}},
     *     {"y": {"z": {"w": 1}}}
     *   ]
     *  }
     */
    public void testGuessMappingRecursiveWithListOfConflictingObjects1() {
        var innerList = List.of(Map.of("x", 1), Map.of("y", Map.of("z", 10)), Map.of("y", Map.of("z", Map.of("w", 1))));
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", innerList);

        Exception e = expectThrows(
            RuntimeException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10)
        );
        assertEquals("Field [key.y.z] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    /**
     * Input:
     * {
     *   "key": [
     *     {"x": 1},
     *     {"y": {"z": {"w": 1}}}
     *     {"y": {"z": 10}},
     *   ]
     *  }
     */
    public void testGuessMappingRecursiveWithListOfConflictingObjects2() {
        var innerList = List.of(Map.of("x", 1), Map.of("y", Map.of("z", Map.of("w", 1))), Map.of("y", Map.of("z", 10)));
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", innerList);

        Exception e = expectThrows(
            RuntimeException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10)
        );
        assertEquals("Field [key.y.z] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    /**
     * Input:
     * {
     *   "key": [
     *     {"x": 1},
     *     {"y": {"z": 10}},
     *     {"y": {"z": {"q": { "w" : 10}}}}
     *   ]
     * }
     */
    public void testGuessMappingRecursiveWithListOfConflictingObjects3() {
        var innerList = List.of(Map.of("x", 1), Map.of("y", Map.of("z", 10)), Map.of("y", Map.of("z", Map.of("q", Map.of("w", 10)))));
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", innerList);

        Exception e = expectThrows(
            RuntimeException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10)
        );
        assertEquals("Field [key.y.z] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    /**
     * Input:
     * {
     *   "key": [
     *     {"l1": {"l2": {"l3": { "l4" : 10}}}}
     *     {"l1": {"l2": 10}},
     *   ]
     * }
     */
    public void testGuessMappingRecursiveWithListOfDifferentObjects4() {
        var innerList = List.of(Map.of("l1", Map.of("l2", Map.of("l3", Map.of("l4", 10)))), Map.of("l1", Map.of("l2", 10)));
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", innerList);

        Exception e = expectThrows(
            RuntimeException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10)
        );
        assertEquals("Field [key.l1.l2] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    /**
     * Input:
     * {
     *  "hosts": { "host": null },
     *  "timestamp": "1478261151445"
     * }
     */
    public void testGuessMappingRecursiveWithNullValue() {
        Map<String, Object> innerMap = new LinkedHashMap<>();
        innerMap.put("host", null);
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("hosts", innerMap);
        input.put("timestamp", "1478261151445");

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
        assertThat("No mappings other than timestamp", mappings.entrySet(), hasSize(1));
    }

    /**
     * Input:
     * {
     *  "hosts": { "host": [] },
     *  "timestamp": "1478261151445"
     * }
     */
    public void testGuessMappingRecursiveWithEmptyList() {
        Map<String, Object> innerMap = new LinkedHashMap<>();
        innerMap.put("host", List.of());
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("hosts", innerMap);
        input.put("timestamp", "1478261151445");

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
        assertThat("No mappings other than timestamp", mappings.entrySet(), hasSize(1));
    }

    /**
     * Input:
     * {
     *  "level1": { "level2" : { "level3": { "...": { "level10": 1}}}},
     * }
     */
    public void testGuessMappingDeeplyNestedRecordsWithinRecursionLimit() {
        int inputDepth = 10;
        Map<String, Object> finalInput = generateDeeplyNestedRecord(inputDepth);

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(finalInput), NOOP_TIMEOUT_CHECKER, null, inputDepth + 1);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "level1.level2.level3.level4.level5.level6.level7.level8.level9.level10", "long");
        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
    }

    /**
     * Input:
     * {
     *  "level1": { "level2" : { "level3": { "...": { "level10": 1}}}},
     * }
     */
    public void testGuessMappingDeeplyNestedRecordsOutsideRecursionLimit() {
        int inputDepth = 10;
        Map<String, Object> finalInput = generateDeeplyNestedRecord(inputDepth);

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(finalInput), NOOP_TIMEOUT_CHECKER, null, inputDepth - 1);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "level1.level2.level3.level4.level5.level6.level7.level8.level9", "object");
        assertThat(
            "Anything beyond the max depth gets serialized into 'object'",
            mappings,
            not(hasKey("level1.level2.level3.level4.level5.level6.level7.level8.level10"))
        );
        assertKeyAndMappedTime(mappings, "timestamp", "date", "epoch_millis");
    }

    public void testGuessMappingsWithMaxDepthLessThanOneThrowsException() {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("field", "value");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, 0)
        );
        assertEquals("Mapping depth limit must be at least 1", e.getMessage());
    }

    /**
     * Input:
     * {
     *   "items": [
     *     {"shallow": 1},
     *     {"deep": {"nestedLong": 1, "nestedObject": {"tooDeep": 42}}}
     *     {"deep": {"nestedLong": 1, "nestedObject": {"tooDeep": 42}}}
     *     {"deep": {"nestedLong": 1, "nestedObject": {"tooDeep": 42}}}
     *   ]
     * }
     */
    public void testGuessMappingMaxDepthReachedForElementInList1() {
        Map<String, Object> shallowElement = Map.of("shallow", 1);
        Map<String, Object> deepElement = Map.of("deep", Map.of("nestedLong", 1, "nestedObject", Map.of("tooDeep", 42)));

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("items", List.of(shallowElement, deepElement, deepElement, deepElement));

        int maxDepth = 3;

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, maxDepth);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "items.shallow", "long");
        assertKeyAndMappedType(mappings, "items.deep.nestedLong", "long");
        // anything beyond the desired depth gets mapped to an object
        assertKeyAndMappedType(mappings, "items.deep.nestedObject", "object");
        assertThat(mappings, not(hasKey("items.deep.nested.tooDeep")));
    }

    /**
     * Input:
     * {
     *   "items": {
     *     "l1" [
     *         {"tooDeep": 1},
     *         {"tooDeep": 1},
     *         {"tooDeep": 1}
     *     ]
     *   }
     * }
     */
    public void testGuessMappingMaxDepthReachedForElementInList2() {
        Map<String, Object> tooDeep = Map.of("tooDeep", 1);
        Map<String, Object> deepElement = Map.of("l1", List.of(tooDeep, tooDeep, tooDeep));

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("items", List.of(deepElement, deepElement, deepElement));

        int maxDepth = 2;

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, maxDepth);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        // anything beyond the desired depth gets mapped to an object
        assertKeyAndMappedType(mappings, "items.l1", "object");
    }

    /**
     * Input:
     * {
     *   "key": [
     *     {"l1": {"l2": 1, "l3": 2}}
     *     {"l1": {"l2": 1, "l3": {"l4": 42}}}
     *   ]
     * }
     */
    public void testGuessMappingRecursiveWithListOfConflictingObjects6() {
        Map<String, Object> deepElement1 = Map.of("l1", Map.of("l2", 1, "l3", 2));
        Map<String, Object> deepElement2 = Map.of("l1", Map.of("l2", 1, "l3", Map.of("l4", 42)));

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", List.of(deepElement1, deepElement2));

        int maxDepth = 6;

        Exception e = expectThrows(
            RuntimeException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, maxDepth)
        );
        assertEquals("Field [key.l1.l3] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    /**
     * Input:
     * {
     *   "key": [
     *     {"l1": {"l2": 1, "l3": {"l4": 42}}}
     *     {"l1": {"l2": 1, "l3": 2}}
     *   ]
     * }
     */
    public void testGuessMappingRecursiveWithListOfConflictingObjects7() {
        Map<String, Object> deepElement1 = Map.of("l1", Map.of("l2", 1, "l3", Map.of("l4", 42)));
        Map<String, Object> deepElement2 = Map.of("l1", Map.of("l2", 1, "l3", 2));

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("key", List.of(deepElement1, deepElement2));

        int maxDepth = 6;

        Exception e = expectThrows(
            RuntimeException.class,
            () -> TextStructureUtils.guessMappingsAndCalculateFieldStats(explanation, List.of(input), NOOP_TIMEOUT_CHECKER, null, maxDepth)
        );
        assertEquals("Field [key.l1.l3] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
    }

    public void testGuessMappingsAndCalculateFieldStats() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time", "2018-05-24 17:28:31,735");
        sample1.put("bar", 42);
        sample1.put("nothing", null);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time", "2018-05-29 11:53:02,837");
        sample2.put("bar", 17);
        sample2.put("nothing", null);

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, Arrays.asList(sample1, sample2), NOOP_TIMEOUT_CHECKER, null, 1);
        assertNotNull(mappingsAndFieldStats);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);
        assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("foo"));
        Map<String, String> expectedTimeMapping = new HashMap<>();
        expectedTimeMapping.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
        expectedTimeMapping.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "yyyy-MM-dd HH:mm:ss,SSS");
        assertEquals(expectedTimeMapping, mappings.get("time"));
        assertEquals(Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("bar"));
        assertNull(mappings.get("nothing"));

        Map<String, FieldStats> fieldStats = mappingsAndFieldStats.v2();
        assertNotNull(fieldStats);
        assertEquals(3, fieldStats.size());
        assertEquals(new FieldStats(2, 2, makeTopHits("not a time", 1, "whatever", 1)), fieldStats.get("foo"));
        assertEquals(
            new FieldStats(
                2,
                2,
                "2018-05-24 17:28:31,735",
                "2018-05-29 11:53:02,837",
                makeTopHits("2018-05-24 17:28:31,735", 1, "2018-05-29 11:53:02,837", 1)
            ),
            fieldStats.get("time")
        );
        assertEquals(new FieldStats(2, 2, 17.0, 42.0, 29.5, 29.5, makeTopHits(17, 1, 42, 1)), fieldStats.get("bar"));
        assertNull(fieldStats.get("nothing"));
    }

    public void testMakeIngestPipelineDefinitionGivenNdJsonWithoutTimestamp() {

        assertNull(
            TextStructureUtils.makeIngestPipelineDefinition(
                null,
                Collections.emptyMap(),
                null,
                Collections.emptyMap(),
                null,
                null,
                false,
                false,
                null
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenNdJsonWithTimestamp() {

        String timestampField = randomAlphaOfLength(10);
        List<String> timestampFormats = randomFrom(
            Collections.singletonList("ISO8601"),
            Arrays.asList("EEE MMM dd HH:mm:ss yyyy", "EEE MMM  d HH:mm:ss yyyy")
        );
        boolean needClientTimezone = randomBoolean();
        boolean needNanosecondPrecision = randomBoolean();
        String ecsCompatibility = randomAlphaOfLength(80);

        Map<String, Object> pipeline = TextStructureUtils.makeIngestPipelineDefinition(
            null,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            timestampField,
            timestampFormats,
            needClientTimezone,
            needNanosecondPrecision,
            ecsCompatibility
        );
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by text structure finder", pipeline.remove("description"));

        List<Map<String, Object>> processors = (List<Map<String, Object>>) pipeline.remove("processors");
        assertNotNull(processors);
        assertEquals(1, processors.size());

        Map<String, Object> dateProcessor = (Map<String, Object>) processors.get(0).get("date");
        assertNotNull(dateProcessor);
        assertEquals(timestampField, dateProcessor.get("field"));
        assertEquals(needClientTimezone, dateProcessor.containsKey("timezone"));
        assertEquals(timestampFormats, dateProcessor.get("formats"));
        if (needNanosecondPrecision) {
            assertEquals(TextStructureUtils.NANOSECOND_DATE_OUTPUT_FORMAT, dateProcessor.get("output_format"));
        } else {
            assertNull(dateProcessor.get("output_format"));
        }

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenDelimitedWithoutTimestamp() {

        Map<String, Object> csvProcessorSettings = DelimitedTextStructureFinderTests.randomCsvProcessorSettings();

        Map<String, Object> pipeline = TextStructureUtils.makeIngestPipelineDefinition(
            null,
            Collections.emptyMap(),
            csvProcessorSettings,
            Collections.emptyMap(),
            null,
            null,
            false,
            false,
            null
        );
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by text structure finder", pipeline.remove("description"));

        List<Map<String, Object>> processors = (List<Map<String, Object>>) pipeline.remove("processors");
        assertNotNull(processors);
        assertEquals(2, processors.size());

        Map<String, Object> csvProcessor = (Map<String, Object>) processors.get(0).get("csv");
        assertNotNull(csvProcessor);
        assertThat(csvProcessor.get("field"), instanceOf(String.class));
        assertThat(csvProcessor.get("target_fields"), instanceOf(List.class));

        Map<String, Object> removeProcessor = (Map<String, Object>) processors.get(1).get("remove");
        assertNotNull(removeProcessor);
        assertThat(csvProcessor.get("field"), equalTo(csvProcessorSettings.get("field")));

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenDelimitedWithFieldInTargetFields() {

        Map<String, Object> csvProcessorSettings = new HashMap<>(DelimitedTextStructureFinderTests.randomCsvProcessorSettings());
        // Hack it so the field to be parsed is also one of the column names
        String firstTargetField = ((List<String>) csvProcessorSettings.get("target_fields")).get(0);
        csvProcessorSettings.put("field", firstTargetField);

        Map<String, Object> pipeline = TextStructureUtils.makeIngestPipelineDefinition(
            null,
            Collections.emptyMap(),
            csvProcessorSettings,
            Collections.emptyMap(),
            null,
            null,
            false,
            false,
            null
        );
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by text structure finder", pipeline.remove("description"));

        List<Map<String, Object>> processors = (List<Map<String, Object>>) pipeline.remove("processors");
        assertNotNull(processors);
        assertEquals(1, processors.size()); // 1 because there's no "remove" processor this time

        Map<String, Object> csvProcessor = (Map<String, Object>) processors.get(0).get("csv");
        assertNotNull(csvProcessor);
        assertThat(csvProcessor.get("field"), equalTo(firstTargetField));
        assertThat(csvProcessor.get("target_fields"), instanceOf(List.class));
        assertThat(csvProcessor.get("ignore_missing"), equalTo(false));

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenDelimitedWithConversion() {

        Map<String, Object> csvProcessorSettings = DelimitedTextStructureFinderTests.randomCsvProcessorSettings();
        boolean expectConversion = randomBoolean();
        String mappingType = expectConversion ? randomFrom("long", "double", "boolean") : randomFrom("keyword", "text", "date");
        String firstTargetField = ((List<String>) csvProcessorSettings.get("target_fields")).get(0);
        Map<String, Object> mappingsForConversions = Collections.singletonMap(
            firstTargetField,
            Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, mappingType)
        );

        Map<String, Object> pipeline = TextStructureUtils.makeIngestPipelineDefinition(
            null,
            Collections.emptyMap(),
            csvProcessorSettings,
            mappingsForConversions,
            null,
            null,
            false,
            false,
            null
        );
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by text structure finder", pipeline.remove("description"));

        List<Map<String, Object>> processors = (List<Map<String, Object>>) pipeline.remove("processors");
        assertNotNull(processors);
        assertEquals(expectConversion ? 3 : 2, processors.size());

        Map<String, Object> csvProcessor = (Map<String, Object>) processors.get(0).get("csv");
        assertNotNull(csvProcessor);
        assertThat(csvProcessor.get("field"), instanceOf(String.class));
        assertThat(csvProcessor.get("target_fields"), instanceOf(List.class));
        assertThat(csvProcessor.get("ignore_missing"), equalTo(false));

        if (expectConversion) {
            Map<String, Object> convertProcessor = (Map<String, Object>) processors.get(1).get("convert");
            assertNotNull(convertProcessor);
            assertThat(convertProcessor.get("field"), equalTo(firstTargetField));
            assertThat(convertProcessor.get("type"), equalTo(mappingType));
            assertThat(convertProcessor.get("ignore_missing"), equalTo(true));
        }

        Map<String, Object> removeProcessor = (Map<String, Object>) processors.get(processors.size() - 1).get("remove");
        assertNotNull(removeProcessor);
        assertThat(removeProcessor.get("field"), equalTo(csvProcessorSettings.get("field")));

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenDelimitedWithTimestamp() {

        Map<String, Object> csvProcessorSettings = DelimitedTextStructureFinderTests.randomCsvProcessorSettings();

        String timestampField = randomAlphaOfLength(10);
        List<String> timestampFormats = randomFrom(
            Collections.singletonList("ISO8601"),
            Arrays.asList("EEE MMM dd HH:mm:ss yyyy", "EEE MMM  d HH:mm:ss yyyy")
        );
        boolean needClientTimezone = randomBoolean();
        boolean needNanosecondPrecision = randomBoolean();

        String ecsCompatibility = randomAlphaOfLength(80);

        Map<String, Object> pipeline = TextStructureUtils.makeIngestPipelineDefinition(
            null,
            Collections.emptyMap(),
            csvProcessorSettings,
            Collections.emptyMap(),
            timestampField,
            timestampFormats,
            needClientTimezone,
            needNanosecondPrecision,
            ecsCompatibility
        );
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by text structure finder", pipeline.remove("description"));

        List<Map<String, Object>> processors = (List<Map<String, Object>>) pipeline.remove("processors");
        assertNotNull(processors);
        assertEquals(3, processors.size());

        Map<String, Object> csvProcessor = (Map<String, Object>) processors.get(0).get("csv");
        assertNotNull(csvProcessor);
        assertThat(csvProcessor.get("field"), instanceOf(String.class));
        assertThat(csvProcessor.get("target_fields"), instanceOf(List.class));
        assertThat(csvProcessor.get("ignore_missing"), equalTo(false));

        Map<String, Object> dateProcessor = (Map<String, Object>) processors.get(1).get("date");
        assertNotNull(dateProcessor);
        assertEquals(timestampField, dateProcessor.get("field"));
        assertEquals(needClientTimezone, dateProcessor.containsKey("timezone"));
        assertEquals(timestampFormats, dateProcessor.get("formats"));
        if (needNanosecondPrecision) {
            assertEquals(TextStructureUtils.NANOSECOND_DATE_OUTPUT_FORMAT, dateProcessor.get("output_format"));
        } else {
            assertNull(dateProcessor.get("output_format"));
        }

        Map<String, Object> removeProcessor = (Map<String, Object>) processors.get(2).get("remove");
        assertNotNull(removeProcessor);
        assertThat(removeProcessor.get("field"), equalTo(csvProcessorSettings.get("field")));

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenSemiStructured() {

        String grokPattern = randomAlphaOfLength(100);
        String timestampField = randomAlphaOfLength(10);
        List<String> timestampFormats = randomFrom(
            Collections.singletonList("ISO8601"),
            Arrays.asList("EEE MMM dd HH:mm:ss yyyy", "EEE MMM  d HH:mm:ss yyyy")
        );
        boolean needClientTimezone = randomBoolean();
        boolean needNanosecondPrecision = randomBoolean();

        String ecsCompatibility = randomAlphaOfLength(80);

        Map<String, Object> pipeline = TextStructureUtils.makeIngestPipelineDefinition(
            grokPattern,
            Collections.emptyMap(),
            null,
            Collections.emptyMap(),
            timestampField,
            timestampFormats,
            needClientTimezone,
            needNanosecondPrecision,
            ecsCompatibility
        );
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by text structure finder", pipeline.remove("description"));

        List<Map<String, Object>> processors = (List<Map<String, Object>>) pipeline.remove("processors");
        assertNotNull(processors);
        assertEquals(3, processors.size());

        Map<String, Object> grokProcessor = (Map<String, Object>) processors.get(0).get("grok");
        assertNotNull(grokProcessor);
        assertEquals("message", grokProcessor.get("field"));
        assertEquals(Collections.singletonList(grokPattern), grokProcessor.get("patterns"));

        Map<String, Object> dateProcessor = (Map<String, Object>) processors.get(1).get("date");
        assertNotNull(dateProcessor);
        assertEquals(timestampField, dateProcessor.get("field"));
        assertEquals(needClientTimezone, dateProcessor.containsKey("timezone"));
        assertEquals(timestampFormats, dateProcessor.get("formats"));
        if (needNanosecondPrecision) {
            assertEquals(TextStructureUtils.NANOSECOND_DATE_OUTPUT_FORMAT, dateProcessor.get("output_format"));
        } else {
            assertNull(dateProcessor.get("output_format"));
        }

        Map<String, Object> removeProcessor = (Map<String, Object>) processors.get(2).get("remove");
        assertNotNull(removeProcessor);
        assertEquals(timestampField, dateProcessor.get("field"));

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    public void testGuessGeoPoint() {
        {
            Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
                Map<String, String> mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList("POINT (-77.03653 38.897676)", "POINT (-50.03653 28.8973)"),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("geo_point"));

                mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList("POINT (-77.03653 38.897676)", "bar"),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("keyword"));
            };

            ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
        }

        // There should be no behavioural change between not specifying a timestamp format at all
        // and explicitly specifying it as the special string "null" (other than performance)
        {
            Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
                Map<String, String> mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList("POINT (-77.03653 38.897676)", "POINT (-50.03653 28.8973)"),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("geo_point"));

                mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList("POINT (-77.03653 38.897676)", "bar"),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("keyword"));
            };

            ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
        }
    }

    public void testGuessGeoShape() {
        {
            Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
                Map<String, String> mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList(
                        "POINT (-77.03653 38.897676)",
                        "LINESTRING (-77.03653 38.897676, -77.009051 38.889939)",
                        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))",
                        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), "
                            + "(100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))",
                        "MULTIPOINT (102.0 2.0, 103.0 2.0)",
                        "MULTILINESTRING ((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0), (100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0),"
                            + " (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8))",
                        "MULTIPOLYGON (((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0, 102.0 2.0)), ((100.0 0.0, 101.0 0.0, 101.0 1.0, "
                            + "100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2)))",
                        "GEOMETRYCOLLECTION (POINT (100.0 0.0), LINESTRING (101.0 0.0, 102.0 1.0))",
                        "BBOX (100.0, 102.0, 2.0, 0.0)"
                    ),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("geo_shape"));

                mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList("POINT (-77.03653 38.897676)", "LINESTRING (-77.03653 38.897676, -77.009051 38.889939)", "bar"),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("keyword"));
            };

            ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
        }

        // There should be no behavioural change between not specifying a timestamp format at all
        // and explicitly specifying it as the special string "null" (other than performance)
        {
            Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
                Map<String, String> mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList(
                        "POINT (-77.03653 38.897676)",
                        "LINESTRING (-77.03653 38.897676, -77.009051 38.889939)",
                        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))",
                        "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0), "
                            + "(100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2))",
                        "MULTIPOINT (102.0 2.0, 103.0 2.0)",
                        "MULTILINESTRING ((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0), (100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0),"
                            + " (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8))",
                        "MULTIPOLYGON (((102.0 2.0, 103.0 2.0, 103.0 3.0, 102.0 3.0, 102.0 2.0)), ((100.0 0.0, 101.0 0.0, 101.0 1.0, "
                            + "100.0 1.0, 100.0 0.0), (100.2 0.2, 100.8 0.2, 100.8 0.8, 100.2 0.8, 100.2 0.2)))",
                        "GEOMETRYCOLLECTION (POINT (100.0 0.0), LINESTRING (101.0 0.0, 102.0 1.0))",
                        "BBOX (100.0, 102.0, 2.0, 0.0)"
                    ),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("geo_shape"));

                mapping = TextStructureUtils.guessScalarMapping(
                    explanation,
                    "foo",
                    Arrays.asList("POINT (-77.03653 38.897676)", "LINESTRING (-77.03653 38.897676, -77.009051 38.889939)", "bar"),
                    NOOP_TIMEOUT_CHECKER,
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                );
                assertThat(mapping.get(TextStructureUtils.MAPPING_TYPE_SETTING), equalTo("keyword"));
            };

            ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
        }
    }

    public void testGuessMappingWithNullValue() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("ERROR", null, "DEBUG")));
    }

    /**
     * Input records:
     * {"a": 1, "b": 2}
     * {"a": 3}
     * {"c": 3}
     */
    public void testGuessMappingsWithMissingFieldInSomeRecords() {
        Map<String, Object> record1 = new LinkedHashMap<>();
        record1.put("a", 1);
        record1.put("b", 2);

        Map<String, Object> record2 = new LinkedHashMap<>();
        record2.put("a", 3);

        Map<String, Object> record3 = new LinkedHashMap<>();
        record3.put("c", 3);

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
            .guessMappingsAndCalculateFieldStats(explanation, List.of(record1, record2, record3), NOOP_TIMEOUT_CHECKER, null, 10);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);

        assertKeyAndMappedType(mappings, "a", "long");
        assertKeyAndMappedType(mappings, "b", "long");
        assertKeyAndMappedType(mappings, "c", "long");

        Map<String, FieldStats> fieldStats = mappingsAndFieldStats.v2();
        assertNotNull(fieldStats);
        assertEquals(2, fieldStats.get("a").getCount());
        assertEquals(1, fieldStats.get("b").getCount());
        assertEquals(1, fieldStats.get("c").getCount());
    }

    /**
     * Generates a deeply nested JSON object.
     * Example for desiredDepth=3, id=1: {"level1": {"level2": {"level3": 1}}}
     */
    private Map<String, Object> generateDeeplyNestedRecord(int desiredDepth) {
        Map<String, Object> input = Map.of("level" + desiredDepth, 1);
        for (int i = desiredDepth - 1; i >= 1; i--) {
            input = Map.of("level" + i, input);
        }
        Map<String, Object> finalInput = new LinkedHashMap<>(input);
        finalInput.put("timestamp", "1478261151445");
        return finalInput;
    }

    private Map<String, String> guessMapping(List<String> explanation, String fieldName, List<Object> fieldValues) {
        @SuppressWarnings("unchecked")
        List<Map<String, ?>> recordsMap = List.of(Map.of(fieldName, fieldValues));
        var mappings = TextStructureUtils.guessMappingsAndCalculateFieldStats(
            explanation,
            recordsMap,
            NOOP_TIMEOUT_CHECKER,
            TextStructureUtils.NULL_TIMESTAMP_FORMAT,
            1
        );

        if (mappings.v1().isEmpty()) {
            return null;
        }

        var fieldMapping = Map.ofEntries(mappings.v1().firstEntry());
        @SuppressWarnings("unchecked")
        var fieldMapping2 = (Map<String, String>) fieldMapping.get(fieldName);
        return fieldMapping2;
    }

    private Map<String, String> guessMapping(List<String> explanation, String fieldName, List<Object> fieldValues, String timestampFormat) {
        @SuppressWarnings("unchecked")
        List<Map<String, ?>> recordsMap = List.of(Map.of(fieldName, fieldValues));
        var mappings = TextStructureUtils.guessMappingsAndCalculateFieldStats(
            explanation,
            recordsMap,
            NOOP_TIMEOUT_CHECKER,
            timestampFormat,
            1
        );

        if (mappings.v1().isEmpty()) {
            return null;
        }

        var fieldMapping = Map.ofEntries(mappings.v1().firstEntry());
        @SuppressWarnings("unchecked")
        var fieldMapping2 = (Map<String, String>) fieldMapping.get(fieldName);
        return fieldMapping2;
    }

    private List<Map<String, Object>> makeTopHits(Object value1, int count1, Object value2, int count2) {
        Map<String, Object> topHit1 = new LinkedHashMap<>();
        topHit1.put("value", value1);
        topHit1.put("count", count1);
        Map<String, Object> topHit2 = new LinkedHashMap<>();
        topHit2.put("value", value2);
        topHit2.put("count", count2);
        return Arrays.asList(topHit1, topHit2);
    }
}
