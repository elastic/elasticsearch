/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class JsonLogFileStructureFinderTests extends LogConfigCreatorTestCase {

    private LogFileStructureFinderFactory factory = new JsonLogFileStructureFinderFactory(TEST_TERMINAL);

    public void testCreateConfigsGivenGoodJson() throws Exception {
        assertTrue(factory.canCreateFromSample(JSON_SAMPLE));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        JsonLogFileStructureFinder structure = (JsonLogFileStructureFinder) factory.createFromSample(TEST_FILE_NAME, TEST_INDEX_NAME,
            "ml-cpp", elasticsearchHost, logstashHost, timezone, JSON_SAMPLE, charset);
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        assertThat(structure.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(structure.getLogstashFromFileConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToIngestPipelineConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToIngestPipelineConfig(),
                containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToIngestPipelineConfig(), containsString(elasticsearchHost));
        assertThat(structure.getIngestPipelineFromFilebeatConfig(), containsString("\"field\": \"timestamp\",\n"));
        assertThat(structure.getIngestPipelineFromFilebeatConfig(), containsString("\"formats\": [ \"UNIX_MS\" ]\n"));
    }
}
