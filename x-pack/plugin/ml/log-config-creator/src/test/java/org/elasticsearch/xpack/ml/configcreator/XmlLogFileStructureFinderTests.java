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

public class XmlLogFileStructureFinderTests extends LogConfigCreatorTestCase {

    private LogFileStructureFinderFactory factory = new XmlLogFileStructureFinderFactory(TEST_TERMINAL);

    public void testCreateConfigsGivenGoodXml() throws Exception {
        assertTrue(factory.canCreateFromSample(XML_SAMPLE));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        String timezone = randomFrom(POSSIBLE_TIMEZONES);
        String elasticsearchHost = randomFrom(POSSIBLE_HOSTNAMES);
        String logstashHost = randomFrom(POSSIBLE_HOSTNAMES);
        XmlLogFileStructureFinder structure = (XmlLogFileStructureFinder) factory.createFromSample(TEST_FILE_NAME, TEST_INDEX_NAME,
            "log4cxx-xml", elasticsearchHost, logstashHost, timezone, XML_SAMPLE, charset);
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("multiline.pattern: '^\\s*<log4j:event'\n"));
        assertThat(structure.getFilebeatToLogstashConfig(), containsString(logstashHost));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"[log4j:event][timestamp]\", \"UNIX_MS\" ]\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString(elasticsearchHost));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString("pattern => \"^\\s*<log4j:event\"\n"));
        assertThat(structure.getLogstashFromFileConfig(), containsString("match => [ \"[log4j:event][timestamp]\", \"UNIX_MS\" ]\n"));
        assertThat(structure.getLogstashFromFileConfig(), not(containsString("timezone =>")));
        assertThat(structure.getLogstashFromFileConfig(), containsString(elasticsearchHost));
    }
}
