/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import com.ibm.icu.text.CharsetMatch;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class LogFileStructureFinderTests extends LogConfigCreatorTestCase {

    private LogFileStructureFinder structureFinder;

    @Before
    public void setup() throws IOException {
        structureFinder = new LogFileStructureFinder(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME, "various");
    }

    public void testFindCharsetGivenCharacterWidths() throws Exception {

        for (Charset charset : Arrays.asList(StandardCharsets.UTF_8, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE)) {
            CharsetMatch charsetMatch = structureFinder.findCharset(new ByteArrayInputStream(TEXT_SAMPLE.getBytes(charset)));
            assertEquals(charset.name(), charsetMatch.getName());
        }
    }

    public void testMakeBestStructureGivenJson() throws Exception {
        assertThat(structureFinder.makeBestStructure("{ \"time\": \"2018-05-17T13:41:23\", \"message\": \"hello\" }",
            StandardCharsets.UTF_8.name()), instanceOf(JsonLogFileStructure.class));
    }

    public void testMakeBestStructureGivenXml() throws Exception {
        assertThat(structureFinder.makeBestStructure("<log time=\"2018-05-17T13:41:23\"><message>hello</message></log>",
            StandardCharsets.UTF_8.name()), instanceOf(XmlLogFileStructure.class));
    }

    public void testMakeBestStructureGivenCsv() throws Exception {
        assertThat(structureFinder.makeBestStructure("time,message\n" +
            "2018-05-17T13:41:23,hello\n", StandardCharsets.UTF_8.name()), instanceOf(SeparatedValuesLogFileStructure.class));
    }

    public void testMakeBestStructureGivenText() throws Exception {
        assertThat(structureFinder.makeBestStructure("[2018-05-17T13:41:23] hello\n", StandardCharsets.UTF_8.name()),
            instanceOf(TextLogFileStructure.class));
    }

    public void testFindLogFileFormatGivenJson() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(JSON_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        // TODO assert
    }

    public void testFindLogFileFormatGivenXml() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(XML_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        // TODO assert
    }

    public void testFindLogFileFormatGivenCsv() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(CSV_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        // TODO assert
    }

    public void testFindLogFileFormatGivenTsv() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(TSV_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        // TODO assert
    }

    public void testFindLogFileFormatGivenText() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(TEXT_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        // TODO assert
    }
}
