/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import static org.hamcrest.Matchers.containsString;

public class XmlLogFileStructureTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory = new XmlLogFileStructureFactory(TEST_TERMINAL);

    public void testCreateConfigsGivenGoodXml() throws Exception {
        assertTrue(factory.canCreateFromSample(XML_SAMPLE));
        XmlLogFileStructure structure = (XmlLogFileStructure) factory.createFromSample(TEST_FILE_NAME, TEST_INDEX_NAME, "log4cxx-xml",
            XML_SAMPLE);
        structure.createConfigs();
        assertThat(structure.getFilebeatToLogstashConfig(), containsString("multiline.pattern: '^\\s*<log4j:event'\n"));
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        assertThat(structure.getLogstashFromStdinConfig(), containsString("pattern => \"^\\s*<log4j:event\"\n"));
        assertThat(structure.getLogstashFromStdinConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
    }
}
