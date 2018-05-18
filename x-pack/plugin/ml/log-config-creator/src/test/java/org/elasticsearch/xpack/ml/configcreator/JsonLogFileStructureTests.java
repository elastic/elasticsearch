/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import static org.hamcrest.Matchers.containsString;

public class JsonLogFileStructureTests extends LogConfigCreatorTestCase {

    private LogFileStructureFactory factory = new JsonLogFileStructureFactory(TEST_TERMINAL);

    public void testCreateConfigsGivenGoodJson() throws Exception {
        assertTrue(factory.canCreateFromSample(JSON_SAMPLE));
        JsonLogFileStructure structure = (JsonLogFileStructure) factory.createFromSample(TEST_FILE_NAME, TEST_INDEX_NAME, "ml-cpp",
            JSON_SAMPLE);
        structure.createConfigs();
        assertThat(structure.getLogstashFromFilebeatConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        assertThat(structure.getLogstashFromStdinConfig(), containsString("match => [ \"timestamp\", \"UNIX_MS\" ]\n"));
        assertThat(structure.getIngestPipelineFromFilebeatConfig(), containsString("\"field\": \"timestamp\",\n"));
        assertThat(structure.getIngestPipelineFromFilebeatConfig(), containsString("\"formats\": [ \"UNIX_MS\" ]\n"));
    }
}
