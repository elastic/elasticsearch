/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.rest;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

public class TextStructureTimestampFormatsIT extends ESRestTestCase {

    public static final String[] ISO_08601_JAVA_FORMATS = new String[] { "yyyy-MM-dd HH:mm:ss" };
    public static final String ISO_08601_TIMESTAMP_GROK_PATTERN = "%{TIMESTAMP_ISO8601:timestamp}";

    public static final String[] TIMESTAMP_YMD_JAVA_FORMATS = new String[] {
        "yyyy/MM/dd HH:mm:ss",
        "yyyy.MM.dd HH:mm:ss",
        "yyyy-MM-dd HH:mm:ss" };
    public static final String TIMESTAMP_YMD_TIMESTAMP_GROK_PATTERN = "%{TIMESTAMP_YMD:timestamp}";

    public static final String[] MONTH_EXPLICIT_NAME_JAVA_FORMATS = new String[] { "MMM d, yyyy" };

    private final String ecsCompatibility;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("x-pack-text-structure")
        .setting("xpack.security.enabled", "false")
        .build();

    public TextStructureTimestampFormatsIT(@Name("ecs_compatibility") String ecsCompatibility) {
        this.ecsCompatibility = ecsCompatibility;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[] { "v1" }, new Object[] { "disabled" });
    }

    public void testTimestampYearYmdSlashFormat() throws IOException {
        // use a multi-line sample to ensure we are detecting ndjson format
        Map<String, Object> responseMap = executeAndVerifyRequest("""
            "2025/07/10 10:30:35"
            "2025/07/10 10:31:42"
            "2025/07/10 10:32:15"
            """, ecsCompatibility);
        verifyTimestampDetected(responseMap, "date");
        verifyTimestampFormat(responseMap, TIMESTAMP_YMD_TIMESTAMP_GROK_PATTERN, TIMESTAMP_YMD_JAVA_FORMATS);
    }

    public void testTimestampYearYmdSlashFormat_WithDotAndMillis() throws IOException {
        // use a multi-line sample to ensure we are detecting ndjson format
        Map<String, Object> responseMap = executeAndVerifyRequest("""
            "2025/07/10 10:30:35.123"
            "2025/07/10 10:31:42.123"
            "2025/07/10 10:32:15.123"
            """, ecsCompatibility);
        verifyTimestampDetected(responseMap, "date");
        verifyTimestampFormat(
            responseMap,
            TIMESTAMP_YMD_TIMESTAMP_GROK_PATTERN,
            "yyyy/MM/dd HH:mm:ss.SSS",
            "yyyy.MM.dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSS"
        );
    }

    public void testTimestampYearYmdSlashFormat_WithSlashAndNanos() throws IOException {
        // use a multi-line sample to ensure we are detecting ndjson format
        Map<String, Object> responseMap = executeAndVerifyRequest("""
            "2025/07/10 10:30:35,123456789"
            "2025/07/10 10:31:42,123456789"
            "2025/07/10 10:32:15,123456789"
            """, ecsCompatibility);
        verifyTimestampDetected(responseMap, "date_nanos");
        verifyTimestampFormat(
            responseMap,
            TIMESTAMP_YMD_TIMESTAMP_GROK_PATTERN,
            "yyyy/MM/dd HH:mm:ss,SSSSSSSSS",
            "yyyy.MM.dd HH:mm:ss,SSSSSSSSS",
            "yyyy-MM-dd HH:mm:ss,SSSSSSSSS"
        );
    }

    public void testTimestampYearYmdDotFormat() throws IOException {
        // use a multi-line sample to ensure we are detecting ndjson format
        Map<String, Object> responseMap = executeAndVerifyRequest("""
            "2025.07.10 10:30:35"
            "2025.07.10 10:31:42"
            "2025.07.10 10:32:15"
            """, ecsCompatibility);
        verifyTimestampDetected(responseMap, "date");
        verifyTimestampFormat(responseMap, TIMESTAMP_YMD_TIMESTAMP_GROK_PATTERN, TIMESTAMP_YMD_JAVA_FORMATS);
    }

    public void testIso08601TimestampFormat() throws IOException {
        // use a multi-line sample to ensure we are detecting ndjson format
        Map<String, Object> responseMap = executeAndVerifyRequest("""
            "2025-07-10 10:30:35"
            "2025-07-10 10:31:42"
            "2025-07-10 10:32:15"
            """, ecsCompatibility);
        verifyTimestampDetected(responseMap, "date");
        // ISO_8601 should have higher priority than TIMESTAMP_YMD
        verifyTimestampFormat(responseMap, ISO_08601_TIMESTAMP_GROK_PATTERN, ISO_08601_JAVA_FORMATS);
    }

    public void testMonthExplicitNameFormat() throws IOException {
        // use a multi-line sample to ensure we are detecting ndjson format
        Map<String, Object> responseMap = executeAndVerifyRequest("""
            "Aug 9, 2025"
            "Aug 10, 2025"
            "Aug 11, 2025"
            """, ecsCompatibility);
        verifyTimestampDetected(responseMap, "date");
        verifyTimestampFormat(responseMap, "CUSTOM_TIMESTAMP", MONTH_EXPLICIT_NAME_JAVA_FORMATS);
    }

    private static Map<String, Object> executeAndVerifyRequest(String sample, String ecsCompatibility) throws IOException {
        Request request = new Request("POST", "/_text_structure/find_structure");
        request.addParameter("ecs_compatibility", ecsCompatibility);
        request.setEntity(new StringEntity(sample, ContentType.APPLICATION_JSON));
        Response response = client().performRequest(request);
        assertOK(response);
        return entityAsMap(response);
    }

    private static void verifyTimestampDetected(Map<String, Object> responseMap, String expectedType) {
        @SuppressWarnings("unchecked")
        Map<String, Object> mappings = (Map<String, Object>) responseMap.get("mappings");
        assertThat(mappings, hasKey("properties"));
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        assertThat(properties, hasKey("@timestamp"));
        @SuppressWarnings("unchecked")
        Map<String, Object> timestamp = (Map<String, Object>) properties.get("@timestamp");
        assertThat(timestamp.get("type"), equalTo(expectedType));
    }

    private static void verifyTimestampFormat(Map<String, Object> responseMap, String expectedGrokPattern, String... expectedJavaFormats) {
        assertThat(responseMap, hasKey("java_timestamp_formats"));
        @SuppressWarnings("unchecked")
        List<String> javaTimestampFormats = (List<String>) responseMap.get("java_timestamp_formats");
        assertThat(javaTimestampFormats, containsInAnyOrder(expectedJavaFormats));
        String grokPattern = (String) responseMap.get("grok_pattern");
        assertThat(grokPattern, containsString(expectedGrokPattern));
    }
}
