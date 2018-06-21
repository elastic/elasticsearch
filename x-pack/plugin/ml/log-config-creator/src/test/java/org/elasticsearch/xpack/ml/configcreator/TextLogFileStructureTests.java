/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.common.util.set.Sets;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class TextLogFileStructureTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory;

    @Before
    public void setup() throws IOException {
        factory = new TextLogFileStructureFactory(TEST_TERMINAL, null);
    }

    public void testCreateConfigsGivenElasticsearchLog() throws Exception {
        assertTrue(factory.canCreateFromSample(TEXT_SAMPLE));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        TextLogFileStructure structure = (TextLogFileStructure) factory.createFromSample(TEST_FILE_NAME, TEST_INDEX_NAME, "es",
            TEXT_SAMPLE, charset);
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(),
            containsString("multiline.pattern: '^\\[\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(),
            containsString("match => { \"message\" => \"\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} \\]" +
                "\\[.*\" }\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"timestamp\", \"ISO8601\" ]\n"));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(),
            containsString("match => { \"message\" => \"\\[%{TIMESTAMP_ISO8601:timestamp}\\]\\[%{LOGLEVEL:loglevel} \\]" +
                "\\[.*\" }\n"));
        assertThat(structure.getLogstashFromFileConfig(), containsString("match => [ \"timestamp\", \"ISO8601\" ]\n"));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToIngestPipelineConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToIngestPipelineConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToIngestPipelineConfig(),
            containsString("multiline.pattern: '^\\[\\b\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'\n"));
        assertThat(structure.getIngestPipelineFromFilebeatConfig(),
            containsString("\"patterns\": [ \"\\\\[%{TIMESTAMP_ISO8601:timestamp}\\\\]\\\\[%{LOGLEVEL:loglevel} \\\\]" +
                "\\\\[.*\" ]\n"));
        assertThat(structure.getIngestPipelineFromFilebeatConfig(), containsString("\"field\": \"timestamp\",\n"));
        assertThat(structure.getIngestPipelineFromFilebeatConfig(), containsString("\"formats\": [ \"ISO8601\" ],\n"));
    }

    public void testCreateMultiLineMessageStartRegexGivenNoPrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^" + simpleDateRegex.replaceFirst("^\\\\b", ""),
                TextLogFileStructure.createMultiLineMessageStartRegex(Collections.emptySet(), simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenOneEmptyPreface() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^" + simpleDateRegex.replaceFirst("^\\\\b", ""),
                TextLogFileStructure.createMultiLineMessageStartRegex(Collections.singleton(""), simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenOneLogLevelPreface() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^\\[.*?\\] \\[" + simpleDateRegex,
                TextLogFileStructure.createMultiLineMessageStartRegex(Collections.singleton("[ERROR] ["), simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyLogLevelPrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("[ERROR] [", "[DEBUG] [");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^\\[.*?\\] \\[" + simpleDateRegex,
                TextLogFileStructure.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyHostnamePrefaces() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("host-1.acme.com|", "my_host.elastic.co|");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^.*?\\|" + simpleDateRegex,
                TextLogFileStructure.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }

    public void testCreateMultiLineMessageStartRegexGivenManyPrefacesIncludingEmpty() {
        for (TimestampFormatFinder.CandidateTimestampFormat candidateTimestampFormat : TimestampFormatFinder.ORDERED_CANDIDATE_FORMATS) {
            Set<String> prefaces = Sets.newHashSet("", "[non-standard] ");
            String simpleDateRegex = candidateTimestampFormat.simplePattern.pattern();
            assertEquals("^.*?" + simpleDateRegex,
                TextLogFileStructure.createMultiLineMessageStartRegex(prefaces, simpleDateRegex));
        }
    }
}
