/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GrokBuiltinPatterns {

    public static final String ECS_COMPATIBILITY_DISABLED = "disabled";
    public static final String ECS_COMPATIBILITY_V1 = "v1";
    public static final List<String> ECS_COMPATIBILITY_MODES = List.of(ECS_COMPATIBILITY_DISABLED, ECS_COMPATIBILITY_V1);

    /**
     * Patterns built in to the grok library.
     */
    private static Map<String, String> LEGACY_PATTERNS;
    private static Map<String, String> ECS_V1_PATTERNS;

    public static synchronized Map<String, String> legacyPatterns() {
        return get(false);
    }

    public static synchronized Map<String, String> ecsV1Patterns() {
        return get(true);
    }

    /**
     * Load built-in patterns.
     */
    public static synchronized Map<String, String> get(boolean ecsCompatibility) {
        if (ecsCompatibility) {
            if (ECS_V1_PATTERNS == null) {
                ECS_V1_PATTERNS = loadEcsPatterns();
            }
            return ECS_V1_PATTERNS;
        } else {
            if (LEGACY_PATTERNS == null) {
                LEGACY_PATTERNS = loadLegacyPatterns();
            }
            return LEGACY_PATTERNS;
        }
    }

    public static Map<String, String> get(String ecsCompatibility) {
        if (isValidEcsCompatibilityMode(ecsCompatibility)) {
            return get(ECS_COMPATIBILITY_V1.equals(ecsCompatibility));
        } else {
            throw new IllegalArgumentException("unsupported ECS compatibility mode [" + ecsCompatibility + "]");
        }
    }

    public static boolean isValidEcsCompatibilityMode(String ecsCompatibility) {
        return ECS_COMPATIBILITY_MODES.contains(ecsCompatibility);
    }

    private static Map<String, String> loadLegacyPatterns() {
        var patternNames = List.of(
            "aws",
            "bacula",
            "bind",
            "bro",
            "exim",
            "firewalls",
            "grok-patterns",
            "haproxy",
            "httpd",
            "java",
            "junos",
            "linux-syslog",
            "maven",
            "mcollective-patterns",
            "mongodb",
            "nagios",
            "postgresql",
            "rails",
            "redis",
            "ruby",
            "squid"
        );
        return loadPatternsFromDirectory(patternNames, "/patterns/legacy/");
    }

    private static Map<String, String> loadEcsPatterns() {
        var patternNames = List.of(
            "aws",
            "bacula",
            "bind",
            "bro",
            "exim",
            "firewalls",
            "grok-patterns",
            "haproxy",
            "httpd",
            "java",
            "junos",
            "linux-syslog",
            "maven",
            "mcollective",
            "mongodb",
            "nagios",
            "postgresql",
            "rails",
            "redis",
            "ruby",
            "squid",
            "zeek"
        );
        return loadPatternsFromDirectory(patternNames, "/patterns/ecs-v1/");
    }

    private static Map<String, String> loadPatternsFromDirectory(List<String> patternNames, String directory) {
        Map<String, String> builtinPatterns = new LinkedHashMap<>();
        for (String pattern : patternNames) {
            try {
                try (InputStream is = GrokBuiltinPatterns.class.getResourceAsStream(directory + pattern)) {
                    loadPatternsFromFile(builtinPatterns, is);
                }
            } catch (IOException e) {
                throw new RuntimeException("failed to load built-in patterns", e);
            }
        }
        return Collections.unmodifiableMap(builtinPatterns);
    }

    private static void loadPatternsFromFile(Map<String, String> patternBank, InputStream inputStream) throws IOException {
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        while ((line = br.readLine()) != null) {
            String trimmedLine = line.replaceAll("^\\s+", "");
            if (trimmedLine.startsWith("#") || trimmedLine.length() == 0) {
                continue;
            }

            String[] parts = trimmedLine.split("\\s+", 2);
            if (parts.length == 2) {
                patternBank.put(parts[0], parts[1]);
            }
        }
    }
}
