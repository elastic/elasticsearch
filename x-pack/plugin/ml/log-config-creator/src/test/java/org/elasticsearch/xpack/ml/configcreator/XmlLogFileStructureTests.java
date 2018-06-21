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

public class XmlLogFileStructureTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory = new XmlLogFileStructureFactory(TEST_TERMINAL);

    public void testCreateConfigsGivenGoodXml() throws Exception {
        assertTrue(factory.canCreateFromSample(XML_SAMPLE));
        String charset = randomFrom(POSSIBLE_CHARSETS);
        XmlLogFileStructure structure = (XmlLogFileStructure) factory.createFromSample(TEST_FILE_NAME, TEST_INDEX_NAME, "log4cxx-xml",
            XML_SAMPLE, charset);
        structure.createConfigs();
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getFilebeatToLogstashConfig(), not(containsString("encoding:")));
        } else {
            assertThat(structure.getFilebeatToLogstashConfig(), containsString("encoding: '" + charset.toLowerCase(Locale.ROOT) + "'"));
        }
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("multiline.pattern: '^\\s*<log4j:event'\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        if (charset.equals(StandardCharsets.UTF_8.name())) {
            assertThat(structure.getLogstashFromFileConfig(), not(containsString("charset =>")));
        } else {
            assertThat(structure.getLogstashFromFileConfig(), containsString("charset => \"" + charset + "\""));
        }
        assertThat(structure.getLogstashFromFileConfig(), containsString("pattern => \"^\\s*<log4j:event\"\n"));
        assertThat(structure.getLogstashFromFileConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
    }
}
