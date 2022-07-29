/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.textstructure.structurefinder.FieldStats;
import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class LogTextStructureFinderTests extends TextStructureTestCase {

    private final TextStructureFinderFactory factory = new LogTextStructureFinderFactory();

    public void testCreateConfigsGivenLowLineMergeSizeLimit() {

        String sample = """
            2019-05-16 16:56:14 line 1 abcdefghijklmnopqrstuvwxyz
            2019-05-16 16:56:14 line 2 abcdefghijklmnopqrstuvwxyz
            continuation line 2.1
            continuation line 2.2
            continuation line 2.3
            continuation line 2.4
            2019-05-16 16:56:14 line 3 abcdefghijklmnopqrstuvwxyz
            """;

        assertTrue(factory.canCreateFromSample(explanation, sample, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.createFromSample(
                explanation,
                sample,
                charset,
                hasByteOrderMarker,
                100,
                TextStructureOverrides.EMPTY_OVERRIDES,
                NOOP_TIMEOUT_CHECKER
            )
        );

        assertEquals(
            "Merging lines into messages resulted in an unacceptably long message. Merged message would have [4] lines and "
                + "[119] characters (limit [100]). If you have messages this big please increase the value of [line_merge_size_limit]. "
                + "Otherwise it probably means the timestamp has been incorrectly detected, so try overriding that.",
            e.getMessage()
        );
    }

    public void testCreateConfigsGivenElasticsearchLog() throws Exception {
        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        TextStructureFinder structureFinder = factory.createFromSample(
            explanation,
            TEXT_SAMPLE,
            charset,
            hasByteOrderMarker,
            TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
            TextStructureOverrides.EMPTY_OVERRIDES,
            NOOP_TIMEOUT_CHECKER
        );

        TextStructure structure = structureFinder.getStructure();

        assertEquals(TextStructure.Format.SEMI_STRUCTURED_TEXT, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\[\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertEquals("\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} \\]\\[.*", structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("ISO8601"), structure.getJodaTimestampFormats());
        FieldStats messageFieldStats = structure.getFieldStats().get("message");
        assertNotNull(messageFieldStats);
        for (String statMessage : messageFieldStats.getTopHits().stream().map(m -> (String) m.get("value")).collect(Collectors.toList())) {
            assertThat(structureFinder.getSampleMessages(), hasItem(statMessage));
        }
        assertEquals(Collections.singleton("properties"), structure.getMappings().keySet());
    }

    public void testCreateConfigsGivenElasticsearchLogAndTimestampFormatOverride() throws Exception {

        String sample = """
            12/31/2018 1:40PM INFO foo
            1/31/2019 11:40AM DEBUG bar
            2/1/2019 11:00PM INFO foo
            2/2/2019 1:23AM DEBUG bar
            """;

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampFormat("M/d/yyyy h:mma").build();

        assertTrue(factory.canCreateFromSample(explanation, sample, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        TextStructureFinder structureFinder = factory.createFromSample(
            explanation,
            sample,
            charset,
            hasByteOrderMarker,
            TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
            overrides,
            NOOP_TIMEOUT_CHECKER
        );

        TextStructure structure = structureFinder.getStructure();

        assertEquals(TextStructure.Format.SEMI_STRUCTURED_TEXT, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\d{1,2}/\\d{1,2}/\\d{4} \\d{1,2}:\\d{2}[AP]M\\b", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertEquals("%{CUSTOM_TIMESTAMP:timestamp} %{LOGLEVEL:loglevel} .*", structure.getGrokPattern());
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("M/d/YYYY h:mma"), structure.getJodaTimestampFormats());
        FieldStats messageFieldStats = structure.getFieldStats().get("message");
        assertNotNull(messageFieldStats);
        for (String statMessage : messageFieldStats.getTopHits().stream().map(m -> (String) m.get("value")).collect(Collectors.toList())) {
            assertThat(structureFinder.getSampleMessages(), hasItem(statMessage));
        }
        assertEquals(Collections.singleton("properties"), structure.getMappings().keySet());
    }

    public void testCreateConfigsGivenElasticsearchLogAndTimestampFieldOverride() throws Exception {

        TextStructureOverrides overrides = TextStructureOverrides.builder().setTimestampField("my_time").build();

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        TextStructureFinder structureFinder = factory.createFromSample(
            explanation,
            TEXT_SAMPLE,
            charset,
            hasByteOrderMarker,
            TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
            overrides,
            NOOP_TIMEOUT_CHECKER
        );

        TextStructure structure = structureFinder.getStructure();

        assertEquals(TextStructure.Format.SEMI_STRUCTURED_TEXT, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\[\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertEquals("\\[%{TIMESTAMP_ISO8601:my_time}\\]\\[%{LOGLEVEL:loglevel} \\]\\[.*", structure.getGrokPattern());
        assertEquals("my_time", structure.getTimestampField());
        assertEquals(Collections.singletonList("ISO8601"), structure.getJodaTimestampFormats());
        FieldStats messageFieldStats = structure.getFieldStats().get("message");
        assertNotNull(messageFieldStats);
        for (String statMessage : messageFieldStats.getTopHits().stream().map(m -> (String) m.get("value")).collect(Collectors.toList())) {
            assertThat(structureFinder.getSampleMessages(), hasItem(statMessage));
        }
        assertEquals(Collections.singleton("properties"), structure.getMappings().keySet());
    }

    public void testCreateConfigsGivenElasticsearchLogAndGrokPatternOverride() throws Exception {

        TextStructureOverrides overrides = TextStructureOverrides.builder()
            .setGrokPattern(
                "\\[%{TIMESTAMP_ISO8601:timestamp}\\]"
                    + "\\[%{LOGLEVEL:loglevel} *\\]\\[%{JAVACLASS:class} *\\] \\[%{HOSTNAME:node}\\] %{JAVALOGMESSAGE:message}"
            )
            .build();

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        TextStructureFinder structureFinder = factory.createFromSample(
            explanation,
            TEXT_SAMPLE,
            charset,
            hasByteOrderMarker,
            TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
            overrides,
            NOOP_TIMEOUT_CHECKER
        );

        TextStructure structure = structureFinder.getStructure();

        assertEquals(TextStructure.Format.SEMI_STRUCTURED_TEXT, structure.getFormat());
        assertEquals(charset, structure.getCharset());
        if (hasByteOrderMarker == null) {
            assertNull(structure.getHasByteOrderMarker());
        } else {
            assertEquals(hasByteOrderMarker, structure.getHasByteOrderMarker());
        }
        assertNull(structure.getExcludeLinesPattern());
        assertEquals("^\\[\\b\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}", structure.getMultilineStartPattern());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getShouldTrimFields());
        assertEquals(
            "\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} *\\]"
                + "\\[%{JAVACLASS:class} *\\] \\[%{HOSTNAME:node}\\] %{JAVALOGMESSAGE:message}",
            structure.getGrokPattern()
        );
        assertEquals("timestamp", structure.getTimestampField());
        assertEquals(Collections.singletonList("ISO8601"), structure.getJodaTimestampFormats());
        FieldStats messageFieldStats = structure.getFieldStats().get("message");
        assertNotNull(messageFieldStats);
        for (String statMessage : messageFieldStats.getTopHits().stream().map(m -> (String) m.get("value")).collect(Collectors.toList())) {
            // In this case the "message" field was output by the Grok pattern, so "message"
            // at the end of the processing will _not_ contain a complete sample message
            assertThat(structureFinder.getSampleMessages(), not(hasItem(statMessage)));
        }
        assertEquals(Collections.singleton("properties"), structure.getMappings().keySet());
    }

    public void testCreateConfigsGivenElasticsearchLogAndImpossibleGrokPatternOverride() {

        // This Grok pattern cannot be matched against the messages in the sample because the fields are in the wrong order
        TextStructureOverrides overrides = TextStructureOverrides.builder()
            .setGrokPattern(
                "\\[%{LOGLEVEL:loglevel} *\\]"
                    + "\\[%{HOSTNAME:node}\\]\\[%{TIMESTAMP_ISO8601:timestamp}\\] \\[%{JAVACLASS:class} *\\] %{JAVALOGMESSAGE:message}"
            )
            .build();

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.createFromSample(
                explanation,
                TEXT_SAMPLE,
                charset,
                hasByteOrderMarker,
                TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
                overrides,
                NOOP_TIMEOUT_CHECKER
            )
        );

        assertEquals(
            "Supplied Grok pattern [\\[%{LOGLEVEL:loglevel} *\\]\\[%{HOSTNAME:node}\\]\\[%{TIMESTAMP_ISO8601:timestamp}\\] "
                + "\\[%{JAVACLASS:class} *\\] %{JAVALOGMESSAGE:message}] does not match sample messages",
            e.getMessage()
        );
    }

    public void testCreateConfigsGivenElasticsearchLogAndInvalidGrokPatternOverride() {

        // This Grok pattern has a low-level syntax error
        TextStructureOverrides overrides = TextStructureOverrides.builder().setGrokPattern("[").build();

        assertTrue(factory.canCreateFromSample(explanation, TEXT_SAMPLE, 0.0));

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.createFromSample(
                explanation,
                TEXT_SAMPLE,
                charset,
                hasByteOrderMarker,
                TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
                overrides,
                NOOP_TIMEOUT_CHECKER
            )
        );

        assertEquals("Supplied Grok pattern [[] cannot be converted to a valid regex", e.getMessage());
        assertNotNull(e.getCause());
        assertEquals("premature end of char-class", e.getCause().getMessage());
    }

    public void testErrorOnIncorrectMessageFormation() {

        // This sample causes problems because the (very weird) primary timestamp format
        // is not detected but a secondary format that only occurs in one line is detected
        String sample = """
            Day 21 Month 1 Year 2019 11:04 INFO [localhost] - starting
            Day 21 Month 1 Year 2019 11:04 INFO [localhost] - startup date [Mon Jan 21 11:04:19 CET 2019]
            Day 21 Month 1 Year 2019 11:04 DEBUG [localhost] - details
            Day 21 Month 1 Year 2019 11:04 DEBUG [localhost] - more details
            Day 21 Month 1 Year 2019 11:04 WARN [localhost] - something went wrong
            """;

        String charset = randomFrom(POSSIBLE_CHARSETS);
        Boolean hasByteOrderMarker = randomHasByteOrderMarker(charset);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> factory.createFromSample(
                explanation,
                sample,
                charset,
                hasByteOrderMarker,
                TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
                TextStructureOverrides.EMPTY_OVERRIDES,
                NOOP_TIMEOUT_CHECKER
            )
        );

        assertEquals(
            "Failed to create more than one message from the sample lines provided. (The last is discarded in "
                + "case the sample is incomplete.) If your sample does contain multiple messages the problem is probably that "
                + "the primary timestamp format has been incorrectly detected, so try overriding it.",
            e.getMessage()
        );
    }

    public void testCreateMultiLineMessageStartRegexGivenNoPrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals(
                "^" + simpleDateRegex.replaceFirst("^\\\\b", ""),
                LogTextStructureFinder.createMultiLineMessageStartRegex(Collections.emptySet(), simpleDateRegex)
            );
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenOneEmptyPreface() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals(
                "^" + simpleDateRegex.replaceFirst("^\\\\b", ""),
                LogTextStructureFinder.createMultiLineMessageStartRegex(Collections.singleton(""), simpleDateRegex)
            );
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenOneLogLevelPreface() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals(
                "^\\[.*?\\] \\[" + simpleDateRegex,
                LogTextStructureFinder.createMultiLineMessageStartRegex(Collections.singleton("[ERROR] ["), simpleDateRegex)
            );
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyLogLevelPrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("[ERROR] [", "[DEBUG] [");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals(
                "^\\[.*?\\] \\[" + simpleDateRegex,
                LogTextStructureFinder.createMultiLineMessageStartRegex(prefaces, simpleDateRegex)
            );
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyHostnamePrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("host-1.acme.com|", "my_host.elastic.co|");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^.*?\\|" + simpleDateRegex, LogTextStructureFinder.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyPrefacesIncludingEmpty() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("", "[non-standard] ");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^.*?" + simpleDateRegex, LogTextStructureFinder.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }
}
