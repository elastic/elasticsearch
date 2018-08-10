/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ml.configcreator.TimestampFormatFinder.TimestampMatch;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;

public class AbstractStructuredLogFileStructureFinderTests extends LogConfigCreatorTestCase {

    private static class TestStructuredLogFileStructureFinder extends AbstractStructuredLogFileStructureFinder {

        TestStructuredLogFileStructureFinder() {
        }
    }

    private final TestStructuredLogFileStructureFinder testStructure = new TestStructuredLogFileStructureFinder();

    public void testSingleSampleSingleField() {
        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Collections.singletonList(sample));
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().dateFormats, contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().grokPatternName);
    }

    public void testSamplesWithSameSingleTimeField() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("field1", "2018-05-24T17:33:39,406");
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().dateFormats, contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().grokPatternName);
    }

    public void testSamplesWithOneSingleTimeFieldDifferentFormat() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("field1", "2018-05-24 17:33:39,406");
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNull(match);
    }

    public void testSamplesWithDifferentSingleTimeField() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("another_field", "2018-05-24T17:33:39,406");
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNull(match);
    }

    public void testSingleSampleManyFieldsOneTimeFormat() {
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("foo", "not a time");
        sample.put("time", "2018-05-24 17:28:31,735");
        sample.put("bar", 42);
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Collections.singletonList(sample));
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().dateFormats, contains("YYYY-MM-dd HH:mm:ss,SSS"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().grokPatternName);
    }

    public void testSamplesWithManyFieldsSameSingleTimeFormat() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time", "2018-05-24 17:28:31,735");
        sample1.put("bar", 42);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time", "2018-05-29 11:53:02,837");
        sample2.put("bar", 17);
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().dateFormats, contains("YYYY-MM-dd HH:mm:ss,SSS"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().grokPatternName);
    }

    public void testSamplesWithManyFieldsSameTimeFieldDifferentTimeFormat() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time", "2018-05-24 17:28:31,735");
        sample1.put("bar", 42);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time", "May 29 2018 11:53:02");
        sample2.put("bar", 17);
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNull(match);
    }

    public void testSamplesWithManyFieldsSameSingleTimeFormatDistractionBefore() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("red_herring", "May 29 2007 11:53:02");
        sample1.put("time", "2018-05-24 17:28:31,735");
        sample1.put("bar", 42);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("red_herring", "whatever");
        sample2.put("time", "2018-05-29 11:53:02,837");
        sample2.put("bar", 17);
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().dateFormats, contains("YYYY-MM-dd HH:mm:ss,SSS"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().grokPatternName);
    }

    public void testSamplesWithManyFieldsSameSingleTimeFormatDistractionAfter() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time", "May 24 2018 17:28:31");
        sample1.put("red_herring", "2018-05-24 17:28:31,735");
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time", "May 29 2018 11:53:02");
        sample2.put("red_herring", "17");
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().dateFormats, contains("MMM dd YYYY HH:mm:ss", "MMM  d YYYY HH:mm:ss"));
        assertEquals("CISCOTIMESTAMP", match.v2().grokPatternName);
    }

    public void testSamplesWithManyFieldsInconsistentTimeFields() {
        Map<String, Object> sample1 = new LinkedHashMap<>();
        sample1.put("foo", "not a time");
        sample1.put("time1", "May 24 2018 17:28:31");
        sample1.put("bar", 17);
        Map<String, Object> sample2 = new LinkedHashMap<>();
        sample2.put("foo", "whatever");
        sample2.put("time2", "May 29 2018 11:53:02");
        sample2.put("bar", 42);
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNull(match);
    }

    public void testSamplesWithManyFieldsInconsistentAndConsistentTimeFields() {
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
        Tuple<String, TimestampMatch> match = testStructure.guessTimestampField(explanation, Arrays.asList(sample1, sample2));
        assertNotNull(match);
        assertEquals("time2", match.v1());
        assertThat(match.v2().dateFormats, contains("MMM dd YYYY HH:mm:ss", "MMM  d YYYY HH:mm:ss"));
        assertEquals("CISCOTIMESTAMP", match.v2().grokPatternName);
    }

    public void testGuessMappingGivenNothing() {
        assertNull(testStructure.guessMapping(explanation, "foo", Collections.emptyList()));
    }

    public void testGuessMappingGivenKeyword() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "keyword");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("ERROR", "INFO", "DEBUG")));
        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "not a date")));
    }

    public void testGuessMappingGivenText() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "text");

        assertEquals(expected,
            testStructure.guessMapping(explanation, "foo", Arrays.asList("a", "the quick brown fox jumped over the lazy dog")));
    }

    public void testGuessMappingGivenIp() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "ip");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("10.0.0.1", "172.16.0.1", "192.168.0.1")));
    }

    public void testGuessMappingGivenDouble() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "double");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("3.14159265359", "0", "-8")));
        // 12345678901234567890 is too long for long
        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("1", "2", "12345678901234567890")));
        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList(3.14159265359, 0.0, 1e-308)));
        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("-1e-1", "-1e308", "1e-308")));
    }

    public void testGuessMappingGivenLong() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "long");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("500", "3", "-3")));
        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList(500, 6, 0)));
    }

    public void testGuessMappingGivenDate() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "date");

        assertEquals(expected,
            testStructure.guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "2018-06-11T13:27:12Z")));
    }

    public void testGuessMappingGivenBoolean() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "boolean");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList("false", "true")));
        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList(true, false)));
    }

    public void testGuessMappingGivenArray() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "long");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList(42, Arrays.asList(1, -99))));

        expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "keyword");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList(new String[]{ "x", "y" }, "z")));
    }

    public void testGuessMappingGivenObject() {
        Map<String, String> expected = Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "object");

        assertEquals(expected, testStructure.guessMapping(explanation, "foo", Arrays.asList(Collections.singletonMap("name", "value1"),
            Collections.singletonMap("name", "value2"))));
    }

    public void testGuessMappingGivenObjectAndNonObject() {
        RuntimeException e = expectThrows(RuntimeException.class, () -> testStructure.guessMapping(explanation, "foo",
            Arrays.asList(Collections.singletonMap("name", "value1"), "value2")));

        assertEquals("Field [foo] has both object and non-object values - this won't work with Elasticsearch", e.getMessage());
    }

    public void testGuessMappings() {
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

        Map<String, Object> mappings = testStructure.guessMappings(explanation, Arrays.asList(sample1, sample2));
        assertNotNull(mappings);
        assertEquals(Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "keyword"), mappings.get("foo"));
        Map<String, String> expectedTimeMapping = new HashMap<>();
        expectedTimeMapping.put(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "date");
        expectedTimeMapping.put(AbstractLogFileStructureFinder.MAPPING_FORMAT_SETTING, "YYYY-MM-dd HH:mm:ss,SSS");
        assertEquals(expectedTimeMapping, mappings.get("time"));
        assertEquals(Collections.singletonMap(AbstractLogFileStructureFinder.MAPPING_TYPE_SETTING, "long"), mappings.get("bar"));
        assertNull(mappings.get("nothing"));
    }
}
