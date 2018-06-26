/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import com.ibm.icu.text.CharsetMatch;
import org.elasticsearch.cli.UserException;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class LogFileStructureFinderTests extends LogConfigCreatorTestCase {

    private static final String TEST_TYPE = "various";

    private static final String FILEBEAT_TO_LOGSTASH_YML = TEST_TYPE + "-filebeat-to-logstash.yml";
    private static final String LOGSTASH_FROM_FILEBEAT_CONF = TEST_TYPE + "-logstash-from-filebeat.conf";
    private static final String LOGSTASH_FROM_FILE_CONF = TEST_TYPE + "-logstash-from-file.conf";
    private static final String FILEBEAT_TO_INGEST_PIPELINE_YML = TEST_TYPE + "-filebeat-to-ingest-pipeline.yml";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_CONSOLE = TEST_TYPE + "-ingest-pipeline-from-filebeat.console";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_SH = TEST_TYPE + "-ingest-pipeline-from-filebeat.sh";
    private static final String INDEX_MAPPINGS_CONSOLE = TEST_TYPE + "-index-mappings.console";
    private static final String INDEX_MAPPINGS_SH = TEST_TYPE + "-index-mappings.sh";

    private LogFileStructureFinder structureFinder;

    @Before
    public void setup() throws IOException {
        structureFinder = new LogFileStructureFinder(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME, TEST_TYPE, "UTC");
    }

    public void testFindCharsetGivenCharacterWidths() throws Exception {

        for (Charset charset : Arrays.asList(StandardCharsets.UTF_8, StandardCharsets.UTF_16LE, StandardCharsets.UTF_16BE)) {
            CharsetMatch charsetMatch = structureFinder.findCharset(new ByteArrayInputStream(TEXT_SAMPLE.getBytes(charset)));
            assertEquals(charset.name(), charsetMatch.getName());
        }
    }

    public void testFindCharsetGivenBinary() throws Exception {

        // This input should never match a single byte character set.  ICU4J will sometimes decide
        // that it matches a double byte character set, hence the two assertion branches.
        int size = 1000;
        byte[] binaryBytes = randomByteArrayOfLength(size);
        for (int i = 0; i < 10; ++i) {
            binaryBytes[randomIntBetween(0, size - 1)] = 0;
        }

        try {
            CharsetMatch charsetMatch = structureFinder.findCharset(new ByteArrayInputStream(binaryBytes));
            assertThat(charsetMatch.getName(), startsWith("UTF-16"));
        } catch (UserException e) {
            assertEquals("Could not determine a usable character encoding for the input - could it be binary data?", e.getMessage());
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

        assertTrue(Files.isRegularFile(outputDirectory.resolve(FILEBEAT_TO_LOGSTASH_YML)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILEBEAT_CONF)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILE_CONF)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(FILEBEAT_TO_INGEST_PIPELINE_YML)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_CONSOLE)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_SH)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_CONSOLE)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_SH)));
    }

    public void testFindLogFileFormatGivenXml() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(XML_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        assertTrue(Files.isRegularFile(outputDirectory.resolve(FILEBEAT_TO_LOGSTASH_YML)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILEBEAT_CONF)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILE_CONF)));
        assertFalse(Files.exists(outputDirectory.resolve(FILEBEAT_TO_INGEST_PIPELINE_YML)));
        assertFalse(Files.exists(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_CONSOLE)));
        assertFalse(Files.exists(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_SH)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_CONSOLE)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_SH)));
    }

    public void testFindLogFileFormatGivenCsv() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(CSV_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        assertTrue(Files.isRegularFile(outputDirectory.resolve(FILEBEAT_TO_LOGSTASH_YML)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILEBEAT_CONF)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILE_CONF)));
        assertFalse(Files.exists(outputDirectory.resolve(FILEBEAT_TO_INGEST_PIPELINE_YML)));
        assertFalse(Files.exists(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_CONSOLE)));
        assertFalse(Files.exists(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_SH)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_CONSOLE)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_SH)));
    }

    public void testFindLogFileFormatGivenTsv() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(TSV_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        assertTrue(Files.isRegularFile(outputDirectory.resolve(FILEBEAT_TO_LOGSTASH_YML)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILEBEAT_CONF)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILE_CONF)));
        assertFalse(Files.exists(outputDirectory.resolve(FILEBEAT_TO_INGEST_PIPELINE_YML)));
        assertFalse(Files.exists(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_CONSOLE)));
        assertFalse(Files.exists(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_SH)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_CONSOLE)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_SH)));
    }

    public void testFindLogFileFormatGivenText() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream =
                 new ByteArrayInputStream(TEXT_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            structureFinder.findLogFileConfigs(inputStream, outputDirectory);
        }

        assertTrue(Files.isRegularFile(outputDirectory.resolve(FILEBEAT_TO_LOGSTASH_YML)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILEBEAT_CONF)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(LOGSTASH_FROM_FILE_CONF)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(FILEBEAT_TO_INGEST_PIPELINE_YML)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_CONSOLE)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INGEST_PIPELINE_FROM_FILEBEAT_SH)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_CONSOLE)));
        assertTrue(Files.isRegularFile(outputDirectory.resolve(INDEX_MAPPINGS_SH)));
    }
}
