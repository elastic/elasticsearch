/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for enhanced timestamp detection covering the formats mentioned in GitHub issue 404
 */
public class EnhancedTimestampDetectionTests extends TextStructureTestCase {

    private ScheduledExecutorService scheduler;
    private TextStructureFinderManager manager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        scheduler = new org.elasticsearch.threadpool.Scheduler.SafeScheduledThreadPoolExecutor(1);
        manager = new TextStructureFinderManager(scheduler);
    }

    @Override
    public void tearDown() throws Exception {
        if (scheduler != null) {
            scheduler.shutdown();
        }
        super.tearDown();
    }

    public void testTimestampYMDSlash() throws Exception {
        verifyTimestampYmdFormat("""
            2025/07/10 10:30:35 INFO An informational message
            2025/07/10 10:31:42 WARN A warning message
            2025/07/10 10:32:15 ERROR An error message
            """);
    }

    public void testTimestampYMDDot() throws Exception {
        verifyTimestampYmdFormat("""
            2025.07.10 10:30:35 INFO An informational message
            2025.07.10 10:31:42 WARN A warning message
            2025.07.10 10:32:15 ERROR An error message
            """);
    }

    public void testTimestampYMDSlashMillisWithDot() throws Exception {
        String sample = """
            2025/07/10 10:30:35.123 INFO An informational message
            2025/07/10 10:31:42.123 WARN A warning message
            2025/07/10 10:32:15.123 ERROR An error message
            """;
        TextStructure structure = findStructure(sample);
        assertThat(structure.getGrokPattern(), containsString("%{TIMESTAMP_YMD:timestamp}"));
        assertNotNull("Should detect timestamp field", structure.getTimestampField());
        assertThat(
            structure.getJavaTimestampFormats(),
            containsInAnyOrder("yyyy/MM/dd HH:mm:ss.SSS", "yyyy.MM.dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss.SSS")
        );
        verifyMappings(structure);
    }

    public void testTimestampYMDDash() throws Exception {
        // ISO_8601 should have higher priority than TIMESTAMP_YMD
        verifyTimestampISO8601Format("""
            2025-07-10 10:30:35 INFO An informational message
            2025-07-10 10:31:42 WARN A warning message
            2025-07-10 10:32:15 ERROR An error message
            """);
    }

    private void verifyTimestampYmdFormat(String sample) throws Exception {
        TextStructure structure = findStructure(sample);
        assertThat(structure.getGrokPattern(), containsString("%{TIMESTAMP_YMD:timestamp}"));
        assertNotNull("Should detect timestamp field", structure.getTimestampField());
        assertThat(
            structure.getJavaTimestampFormats(),
            containsInAnyOrder("yyyy/MM/dd HH:mm:ss", "yyyy.MM.dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss")
        );
        verifyMappings(structure);
    }

    private void verifyTimestampISO8601Format(String sample) throws Exception {
        TextStructure structure = findStructure(sample);
        assertThat(structure.getGrokPattern(), containsString("%{TIMESTAMP_ISO8601:timestamp}"));
        assertNotNull("Should detect timestamp field", structure.getTimestampField());
        assertThat(structure.getJavaTimestampFormats(), containsInAnyOrder("yyyy-MM-dd HH:mm:ss"));
        verifyMappings(structure);
    }

    private static void verifyMappings(TextStructure structure) {
        Map<String, Object> mappings = structure.getMappings();
        assertNotNull("Should have mappings", mappings);
        Object propertiesValue = mappings.get("properties");
        assertThat(propertiesValue, instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) propertiesValue;

        // Verify @timestamp field is properly configured
        @SuppressWarnings("unchecked")
        Map<String, Object> timestampMapping = (Map<String, Object>) properties.get("@timestamp");
        assertThat("@timestamp should be date type", timestampMapping.get("type"), equalTo("date"));
        @SuppressWarnings("unchecked")
        Map<String, Object> logLevelMapping = (Map<String, Object>) properties.get("log.level");
        assertThat("log.level should be keyword type", logLevelMapping.get("type"), equalTo("keyword"));
        @SuppressWarnings("unchecked")
        Map<String, Object> messageMapping = (Map<String, Object>) properties.get("message");
        assertThat("message should be text type", messageMapping.get("type"), equalTo("text"));
    }

    /**
     * Helper method to find text structure using the manager
     */
    private TextStructure findStructure(String sample) throws Exception {
        // Convert the string sample to an InputStream as required by the method signature
        byte[] sampleBytes = sample.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.io.ByteArrayInputStream inputStream = new java.io.ByteArrayInputStream(sampleBytes);

        // Create TextStructureOverrides with ECS compatibility enabled
        TextStructureOverrides overrides = new TextStructureOverrides.Builder().setEcsCompatibility("v1").build();

        TextStructureFinder finder = manager.findTextStructure(
            explanation,
            TextStructureFinderManager.DEFAULT_IDEAL_SAMPLE_LINE_COUNT,
            TextStructureFinderManager.DEFAULT_LINE_MERGE_SIZE_LIMIT,
            inputStream,
            overrides,
            org.elasticsearch.core.TimeValue.timeValueSeconds(30)
        );

        return finder.getStructure();
    }
}
