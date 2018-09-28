/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinder;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinderManager;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class LogConfigCreatorTests extends LogConfigCreatorTestCase {

    private static final int IDEAL_SAMPLE_LINE_COUNT = 1000;

    private static final String TEST_TYPE = "various";

    private static final String FILEBEAT_TO_LOGSTASH_YML = TEST_TYPE + "-filebeat-to-logstash.yml";
    private static final String LOGSTASH_FROM_FILEBEAT_CONF = TEST_TYPE + "-logstash-from-filebeat.conf";
    private static final String LOGSTASH_FROM_FILE_CONF = TEST_TYPE + "-logstash-from-file.conf";
    private static final String FILEBEAT_TO_INGEST_PIPELINE_YML = TEST_TYPE + "-filebeat-to-ingest-pipeline.yml";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_CONSOLE = TEST_TYPE + "-ingest-pipeline-from-filebeat.console";
    private static final String INGEST_PIPELINE_FROM_FILEBEAT_SH = TEST_TYPE + "-ingest-pipeline-from-filebeat.sh";
    private static final String INDEX_MAPPINGS_CONSOLE = TEST_TYPE + "-index-mappings.console";
    private static final String INDEX_MAPPINGS_SH = TEST_TYPE + "-index-mappings.sh";

    private ScheduledExecutorService scheduler;
    private FileStructureFinderManager structureFinderManager;
    private LogConfigWriter logConfigWriter;

    @Before
    public void setup() throws IOException {
        scheduler = new ScheduledThreadPoolExecutor(1);
        structureFinderManager = new FileStructureFinderManager(scheduler);
        logConfigWriter = new LogConfigWriter(TEST_TERMINAL, null, TEST_FILE_NAME, TEST_INDEX_NAME, TEST_TYPE,
            randomFrom(POSSIBLE_HOSTNAMES), randomFrom(POSSIBLE_HOSTNAMES), "UTC");
    }

    @After
    public void shutdownScheduler() {
        scheduler.shutdown();
    }

    public void testFindLogFileFormatGivenJson() throws Exception {
        Path outputDirectory = createTempDir();

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(JSON_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            FileStructureFinder logFileStructureFinder =
                structureFinderManager.findFileStructure(explanation, IDEAL_SAMPLE_LINE_COUNT, inputStream);
            logConfigWriter.writeConfigs(logFileStructureFinder.getStructure(), logFileStructureFinder.getSampleMessages(),
                outputDirectory);
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

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(XML_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            FileStructureFinder logFileStructureFinder =
                structureFinderManager.findFileStructure(explanation, IDEAL_SAMPLE_LINE_COUNT, inputStream);
            logConfigWriter.writeConfigs(logFileStructureFinder.getStructure(), logFileStructureFinder.getSampleMessages(),
                outputDirectory);
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

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(CSV_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            FileStructureFinder logFileStructureFinder =
                structureFinderManager.findFileStructure(explanation, IDEAL_SAMPLE_LINE_COUNT, inputStream);
            logConfigWriter.writeConfigs(logFileStructureFinder.getStructure(), logFileStructureFinder.getSampleMessages(),
                outputDirectory);
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

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(TSV_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            FileStructureFinder logFileStructureFinder =
                structureFinderManager.findFileStructure(explanation, IDEAL_SAMPLE_LINE_COUNT, inputStream);
            logConfigWriter.writeConfigs(logFileStructureFinder.getStructure(), logFileStructureFinder.getSampleMessages(),
                outputDirectory);
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

        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(TEXT_SAMPLE.getBytes(StandardCharsets.UTF_8))) {
            FileStructureFinder logFileStructureFinder =
                structureFinderManager.findFileStructure(explanation, IDEAL_SAMPLE_LINE_COUNT, inputStream);
            logConfigWriter.writeConfigs(logFileStructureFinder.getStructure(), logFileStructureFinder.getSampleMessages(),
                outputDirectory);
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
