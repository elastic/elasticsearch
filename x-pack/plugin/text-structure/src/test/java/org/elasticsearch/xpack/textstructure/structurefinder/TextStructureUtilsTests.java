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
import static org.hamcrest.Matchers.instanceOf;

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
        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            assertNull(guessMapping(explanation, "foo", Collections.emptyList(), ecsCompatibility));
            assertNull(
                guessMapping(explanation, "foo", Collections.emptyList(), ecsCompatibility, TextStructureUtils.NULL_TIMESTAMP_FORMAT)
            );
        };
        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenKeyword() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword");

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("ERROR", "INFO", "DEBUG"), ecsCompatibility));
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "not a date"), ecsCompatibility));
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("ERROR", "INFO", "DEBUG"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("2018-06-11T13:26:47Z", "not a date"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenText() {

        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "text");

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            assertEquals(
                expected,
                guessMapping(explanation, "foo", Arrays.asList("a", "the quick brown fox jumped over the lazy dog"), ecsCompatibility)
            );
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("a", "the quick brown fox jumped over the lazy dog"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenIp() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "ip");

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            assertEquals(
                expected,
                guessMapping(explanation, "foo", Arrays.asList("10.0.0.1", "172.16.0.1", "192.168.0.1"), ecsCompatibility)
            );
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("10.0.0.1", "172.16.0.1", "192.168.0.1"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenDouble() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "double");

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("3.14159265359", "0", "-8"), ecsCompatibility));
            // 12345678901234567890 is too long for long
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("1", "2", "12345678901234567890"), ecsCompatibility));
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(3.14159265359, 0.0, 1e-308), ecsCompatibility));
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("-1e-1", "-1e308", "1e-308"), ecsCompatibility));

            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("3.14159265359", "0", "-8"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
            // 12345678901234567890 is too long for long
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("1", "2", "12345678901234567890"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList(3.14159265359, 0.0, 1e-308),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("-1e-1", "-1e308", "1e-308"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenLong() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long");

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("500", "3", "-3"), ecsCompatibility));
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(500, 6, 0), ecsCompatibility));
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList("500", "3", "-3"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
            assertEquals(
                expected,
                guessMapping(explanation, "foo", Arrays.asList(500, 6, 0), ecsCompatibility, TextStructureUtils.NULL_TIMESTAMP_FORMAT)
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenDate() {
        {
            Map<String, String> expected = new HashMap<>();
            expected.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
            expected.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");

            Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
                assertEquals(
                    expected,
                    guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "2018-06-11T13:27:12Z"), ecsCompatibility)
                );
            };

            ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
        }
        {
            // The special value of "null" for the timestamp format indicates that the analysis
            // of semi-structured text should assume the absence of any timestamp.
            // In the case of structured text, there may be timestamps present in multiple fields
            // which we want the analysis to identify. For now we don't want the user supplied timestamp
            // format override to affect this behaviour, hence this check.
            Map<String, String> expected = new HashMap<>();
            expected.put(TextStructureUtils.MAPPING_TYPE_SETTING, "date");
            expected.put(TextStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");

            Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
                assertEquals(
                    expected,
                    guessMapping(
                        explanation,
                        "foo",
                        Arrays.asList("2018-06-11T13:26:47Z", "2018-06-11T13:27:12Z"),
                        ecsCompatibility,
                        TextStructureUtils.NULL_TIMESTAMP_FORMAT
                    )
                );
            };

            ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
        }
    }

    public void testGuessMappingGivenBoolean() {
        Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "boolean");

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("false", "true"), ecsCompatibility));
            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(true, false), ecsCompatibility));
            assertEquals(
                expected,
                guessMapping(explanation, "foo", Arrays.asList("false", "true"), ecsCompatibility, TextStructureUtils.NULL_TIMESTAMP_FORMAT)
            );
            assertEquals(
                expected,
                guessMapping(explanation, "foo", Arrays.asList(true, false), ecsCompatibility, TextStructureUtils.NULL_TIMESTAMP_FORMAT)
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenArray() {

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {

            Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "long");

            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(42, Arrays.asList(1, -99)), ecsCompatibility));
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList(42, Arrays.asList(1, -99)),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );

            expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "keyword");

            assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(new String[] { "x", "y" }, "z"), ecsCompatibility));
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList(new String[] { "x", "y" }, "z"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenObject() {
        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            Map<String, String> expected = Collections.singletonMap(TextStructureUtils.MAPPING_TYPE_SETTING, "object");

            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList(Collections.singletonMap("name", "value1"), Collections.singletonMap("name", "value2")),
                    ecsCompatibility
                )
            );
            assertEquals(
                expected,
                guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList(Collections.singletonMap("name", "value1"), Collections.singletonMap("name", "value2")),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
    }

    public void testGuessMappingGivenObjectAndNonObject() {
        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            RuntimeException e = expectThrows(
                RuntimeException.class,
                () -> guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList(Collections.singletonMap("name", "value1"), "value2"),
                    ecsCompatibility
                )
            );
            assertEquals("Field [foo] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());

            e = expectThrows(
                RuntimeException.class,
                () -> guessMapping(
                    explanation,
                    "foo",
                    Arrays.asList(Collections.singletonMap("name", "value1"), "value2"),
                    ecsCompatibility,
                    TextStructureUtils.NULL_TIMESTAMP_FORMAT
                )
            );
            assertEquals("Field [foo] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
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

        Consumer<Boolean> testGuessMappingGivenEcsCompatibility = (ecsCompatibility) -> {
            Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats = TextStructureUtils
                .guessMappingsAndCalculateFieldStats(explanation, Arrays.asList(sample1, sample2), NOOP_TIMEOUT_CHECKER, ecsCompatibility);
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
        };

        ecsCompatibilityModes.forEach(testGuessMappingGivenEcsCompatibility);
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

    private Map<String, String> guessMapping(
        List<String> explanation,
        String fieldName,
        List<Object> fieldValues,
        boolean ecsCompatibility,
        String timestampFormatOverride
    ) {
        Tuple<Map<String, String>, FieldStats> mappingAndFieldStats = TextStructureUtils.guessMappingAndCalculateFieldStats(
            explanation,
            fieldName,
            fieldValues,
            NOOP_TIMEOUT_CHECKER,
            ecsCompatibility,
            timestampFormatOverride
        );
        return (mappingAndFieldStats == null) ? null : mappingAndFieldStats.v1();
    }

    private Map<String, String> guessMapping(
        List<String> explanation,
        String fieldName,
        List<Object> fieldValues,
        boolean ecsCompatibility
    ) {
        Tuple<Map<String, String>, FieldStats> mappingAndFieldStats = TextStructureUtils.guessMappingAndCalculateFieldStats(
            explanation,
            fieldName,
            fieldValues,
            NOOP_TIMEOUT_CHECKER,
            ecsCompatibility
        );
        return (mappingAndFieldStats == null) ? null : mappingAndFieldStats.v1();
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
