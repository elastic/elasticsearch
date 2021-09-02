/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EnrollmentTokenTests extends ESTestCase {
    EnrollmentToken createEnrollmentToken() {
        final String apiKey = randomAlphaOfLength(16);
        final String fingerprint = randomAlphaOfLength(64);
        final String version = randomAlphaOfLength(5);
        final List<String> boundAddresses = Arrays.asList(generateRandomStringArray(4, randomIntBetween(2, 32), false));
        return new EnrollmentToken(apiKey, fingerprint, version, boundAddresses);
    }

    public void testEnrollmentToken() throws Exception {
        EnrollmentToken enrollmentToken = createEnrollmentToken();
        final String apiKey = enrollmentToken.getApiKey();
        final String fingerprint = enrollmentToken.getFingerprint();
        final String version = enrollmentToken.getVersion();
        final List<String> boundAddresses = enrollmentToken.getBoundAddress();
        final String jsonString = enrollmentToken.getRaw();
        final String encoded = enrollmentToken.getEncoded();
        final Map<String, String> enrollmentMap;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, jsonString)) {
            final Map<String, Object> info = parser.map();
            assertNotEquals(info, null);
            enrollmentMap = info.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
        }
        assertEquals(enrollmentMap.get("key"), apiKey);
        assertEquals(enrollmentMap.get("fgr"), fingerprint);
        assertEquals(enrollmentMap.get("ver"), version);
        assertEquals(enrollmentMap.get("adr"), "[" + boundAddresses.stream().collect(Collectors.joining(", ")) + "]");
        assertEquals(new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8), jsonString);
    }
}
