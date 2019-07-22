/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.filestructurefinder;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FieldStats;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static org.elasticsearch.xpack.ml.filestructurefinder.FileStructureOverrides.EMPTY_OVERRIDES;
import static org.hamcrest.Matchers.contains;

public class FileStructureUtilsTests extends FileStructureTestCase {

    public void testMoreLikelyGivenText() {
        assertTrue(FileStructureUtils.isMoreLikelyTextThanKeyword("the quick brown fox jumped over the lazy dog"));
        assertTrue(FileStructureUtils.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(257, 10000)));
    }

    public void testMoreLikelyGivenKeyword() {
        assertFalse(FileStructureUtils.isMoreLikelyTextThanKeyword("1"));
        assertFalse(FileStructureUtils.isMoreLikelyTextThanKeyword("DEBUG"));
        assertFalse(FileStructureUtils.isMoreLikelyTextThanKeyword(randomAlphaOfLengthBetween(1, 256)));
    }

    public void testGuessTimestampGivenSingleSampleSingleField() {
        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndConsistentTimeFieldOverride() {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setTimestampField("field1").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample),
            overrides, NOOP_TIMEOUT_CHECKER);
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndImpossibleTimeFieldOverride() {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setTimestampField("field2").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> FileStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample), overrides,
                NOOP_TIMEOUT_CHECKER));

        assertEquals("Specified timestamp field [field2] is not present in record [{field1=2018-05-24T17:28:31,735}]", e.getMessage());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndConsistentTimeFormatOverride() {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setTimestampFormat("ISO8601").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample),
            overrides, NOOP_TIMEOUT_CHECKER);
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSingleSampleSingleFieldAndImpossibleTimeFormatOverride() {

        FileStructureOverrides overrides = FileStructureOverrides.builder().setTimestampFormat("EEE MMM dd HH:mm:ss yyyy").build();

        Map<String, String> sample = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> FileStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample), overrides,
                NOOP_TIMEOUT_CHECKER));

        assertEquals("Specified timestamp format [EEE MMM dd HH:mm:ss yyyy] does not match for record [{field1=2018-05-24T17:28:31,735}]",
            e.getMessage());
    }

    public void testGuessTimestampGivenSamplesWithSameSingleTimeField() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("field1", "2018-05-24T17:33:39,406");
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
        assertNotNull(match);
        assertEquals("field1", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("ISO8601"));
        assertEquals("TIMESTAMP_ISO8601", match.v2().getGrokPatternName());
    }

    public void testGuessTimestampGivenSamplesWithOneSingleTimeFieldDifferentFormat() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("field1", "Thu May 24 17:33:39 2018");
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
        assertNull(match);
    }

    public void testGuessTimestampGivenSamplesWithDifferentSingleTimeField() {
        Map<String, String> sample1 = Collections.singletonMap("field1", "2018-05-24T17:28:31,735");
        Map<String, String> sample2 = Collections.singletonMap("another_field", "2018-05-24T17:33:39,406");
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
        assertNull(match);
    }

    public void testGuessTimestampGivenSingleSampleManyFieldsOneTimeFormat() {
        Map<String, Object> sample = new LinkedHashMap<>();
        sample.put("foo", "not a time");
        sample.put("time", "2018-05-24 17:28:31,735");
        sample.put("bar", 42);
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Collections.singletonList(sample),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
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
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
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
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
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
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
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
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
        assertNotNull(match);
        assertEquals("time", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("MMM dd yyyy HH:mm:ss", "MMM  d yyyy HH:mm:ss"));
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
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
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
        Tuple<String, TimestampFormatFinder> match = FileStructureUtils.guessTimestampField(explanation, Arrays.asList(sample1, sample2),
            EMPTY_OVERRIDES, NOOP_TIMEOUT_CHECKER);
        assertNotNull(match);
        assertEquals("time2", match.v1());
        assertThat(match.v2().getJavaTimestampFormats(), contains("MMM dd yyyy HH:mm:ss", "MMM  d yyyy HH:mm:ss"));
        assertEquals("CISCOTIMESTAMP", match.v2().getGrokPatternName());
    }

    public void testGuessMappingGivenNothing() {
        assertNull(guessMapping(explanation, "foo", Collections.emptyList()));
    }

    public void testGuessMappingGivenKeyword() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "keyword");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("ERROR", "INFO", "DEBUG")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "not a date")));
    }

    public void testGuessMappingGivenText() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "text");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("a", "the quick brown fox jumped over the lazy dog")));
    }

    public void testGuessMappingGivenIp() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "ip");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("10.0.0.1", "172.16.0.1", "192.168.0.1")));
    }

    public void testGuessMappingGivenDouble() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "double");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("3.14159265359", "0", "-8")));
        // 12345678901234567890 is too long for long
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("1", "2", "12345678901234567890")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(3.14159265359, 0.0, 1e-308)));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("-1e-1", "-1e308", "1e-308")));
    }

    public void testGuessMappingGivenLong() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "long");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("500", "3", "-3")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(500, 6, 0)));
    }

    public void testGuessMappingGivenDate() {
        Map<String, String> expected = new HashMap<>();
        expected.put(FileStructureUtils.MAPPING_TYPE_SETTING, "date");
        expected.put(FileStructureUtils.MAPPING_FORMAT_SETTING, "iso8601");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("2018-06-11T13:26:47Z", "2018-06-11T13:27:12Z")));
    }

    public void testGuessMappingGivenBoolean() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "boolean");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList("false", "true")));
        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(true, false)));
    }

    public void testGuessMappingGivenArray() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "long");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(42, Arrays.asList(1, -99))));

        expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "keyword");

        assertEquals(expected, guessMapping(explanation, "foo", Arrays.asList(new String[]{ "x", "y" }, "z")));
    }

    public void testGuessMappingGivenObject() {
        Map<String, String> expected = Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "object");

        assertEquals(expected, guessMapping(explanation, "foo",
            Arrays.asList(Collections.singletonMap("name", "value1"), Collections.singletonMap("name", "value2"))));
    }

    public void testGuessMappingGivenObjectAndNonObject() {
        RuntimeException e = expectThrows(RuntimeException.class, () -> guessMapping(explanation,
            "foo", Arrays.asList(Collections.singletonMap("name", "value1"), "value2")));

        assertEquals("Field [foo] has both object and non-object values - this is not supported by Elasticsearch", e.getMessage());
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

        Tuple<SortedMap<String, Object>, SortedMap<String, FieldStats>> mappingsAndFieldStats =
            FileStructureUtils.guessMappingsAndCalculateFieldStats(explanation, Arrays.asList(sample1, sample2), NOOP_TIMEOUT_CHECKER);
        assertNotNull(mappingsAndFieldStats);

        Map<String, Object> mappings = mappingsAndFieldStats.v1();
        assertNotNull(mappings);
        assertEquals(Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "keyword"), mappings.get("foo"));
        Map<String, String> expectedTimeMapping = new HashMap<>();
        expectedTimeMapping.put(FileStructureUtils.MAPPING_TYPE_SETTING, "date");
        expectedTimeMapping.put(FileStructureUtils.MAPPING_FORMAT_SETTING, "yyyy-MM-dd HH:mm:ss,SSS");
        assertEquals(expectedTimeMapping, mappings.get("time"));
        assertEquals(Collections.singletonMap(FileStructureUtils.MAPPING_TYPE_SETTING, "long"), mappings.get("bar"));
        assertNull(mappings.get("nothing"));

        Map<String, FieldStats> fieldStats = mappingsAndFieldStats.v2();
        assertNotNull(fieldStats);
        assertEquals(3, fieldStats.size());
        assertEquals(new FieldStats(2, 2, makeTopHits("not a time", 1, "whatever", 1)), fieldStats.get("foo"));
        assertEquals(new FieldStats(2, 2, "2018-05-24 17:28:31,735", "2018-05-29 11:53:02,837",
            makeTopHits("2018-05-24 17:28:31,735", 1, "2018-05-29 11:53:02,837", 1)), fieldStats.get("time"));
        assertEquals(new FieldStats(2, 2, 17.0, 42.0, 29.5, 29.5, makeTopHits(17, 1, 42, 1)), fieldStats.get("bar"));
        assertNull(fieldStats.get("nothing"));
    }

    public void testMakeIngestPipelineDefinitionGivenStructuredWithoutTimestamp() {

        assertNull(FileStructureUtils.makeIngestPipelineDefinition(null, Collections.emptyMap(), null, null, false));
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenStructuredWithTimestamp() {

        String timestampField = randomAlphaOfLength(10);
        List<String> timestampFormats = randomFrom(Collections.singletonList("ISO8601"),
            Arrays.asList("EEE MMM dd HH:mm:ss yyyy", "EEE MMM  d HH:mm:ss yyyy"));
        boolean needClientTimezone = randomBoolean();

        Map<String, Object> pipeline = FileStructureUtils.makeIngestPipelineDefinition(null, Collections.emptyMap(), timestampField,
            timestampFormats, needClientTimezone);
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by file structure finder", pipeline.remove("description"));

        List<Map<String, Object>> processors = (List<Map<String, Object>>) pipeline.remove("processors");
        assertNotNull(processors);
        assertEquals(1, processors.size());

        Map<String, Object> dateProcessor = (Map<String, Object>) processors.get(0).get("date");
        assertNotNull(dateProcessor);
        assertEquals(timestampField, dateProcessor.get("field"));
        assertEquals(needClientTimezone, dateProcessor.containsKey("timezone"));
        assertEquals(timestampFormats, dateProcessor.get("formats"));

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    @SuppressWarnings("unchecked")
    public void testMakeIngestPipelineDefinitionGivenSemiStructured() {

        String grokPattern = randomAlphaOfLength(100);
        String timestampField = randomAlphaOfLength(10);
        List<String> timestampFormats = randomFrom(Collections.singletonList("ISO8601"),
            Arrays.asList("EEE MMM dd HH:mm:ss yyyy", "EEE MMM  d HH:mm:ss yyyy"));
        boolean needClientTimezone = randomBoolean();

        Map<String, Object> pipeline = FileStructureUtils.makeIngestPipelineDefinition(grokPattern, Collections.emptyMap(), timestampField,
            timestampFormats, needClientTimezone);
        assertNotNull(pipeline);

        assertEquals("Ingest pipeline created by file structure finder", pipeline.remove("description"));

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

        Map<String, Object> removeProcessor = (Map<String, Object>) processors.get(2).get("remove");
        assertNotNull(removeProcessor);
        assertEquals(timestampField, dateProcessor.get("field"));

        // After removing the two expected fields there should be nothing left in the pipeline
        assertEquals(Collections.emptyMap(), pipeline);
    }

    private Map<String, String> guessMapping(List<String> explanation, String fieldName, List<Object> fieldValues) {
        Tuple<Map<String, String>, FieldStats> mappingAndFieldStats = FileStructureUtils.guessMappingAndCalculateFieldStats(explanation,
            fieldName, fieldValues, NOOP_TIMEOUT_CHECKER);
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
