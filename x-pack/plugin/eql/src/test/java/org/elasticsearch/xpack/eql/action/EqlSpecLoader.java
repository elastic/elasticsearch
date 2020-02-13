/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import io.netty.util.internal.StringUtil;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EqlSpecLoader {
    public static List<EqlSpec> load(String path, boolean supported) throws Exception {
        try (InputStream is = EqlSpecLoader.class.getResourceAsStream(path)) {
            return readFromStream(is, supported);
        }
    }

    private static int[] readExpectedEventIds(String line, BufferedReader reader) throws Exception {
        String arr[] = readArray(line);
        int ids[] = new int[arr.length];

        for (int i = 0; i < arr.length; i++) {
            ids[i] = Integer.parseInt(arr[i]);
        }
        return ids;
    }

    private static String[] readTags(String line) throws Exception {
        String arr[] = readArray(line);
        for (int i = 0; i < arr.length; i++) {
            String s = arr[i];
            if (s.startsWith("\"") || s.startsWith("'")) {
                s = s.substring(1);
            }
            if (s.endsWith("\"") || s.endsWith("'")) {
                s = s.substring(0, s.length() - 1);
            }
            arr[i] = s.trim();
        }
        return arr;
    }

    private static String readValueLine(String line) throws Exception {
        int idx = line.indexOf("=");
        if (idx == -1) {
            throw new IllegalArgumentException("Invalid string value: " + line);
        }
        return line.substring(idx + 1).trim();
    }

    private static String[] readArray(String line) throws Exception {
        line = readValueLine(line);
        if (!line.startsWith("[") && !line.endsWith("]")) {
            throw new IllegalArgumentException("Invalid array string value: " + line);
        }
        String arr[] = line.substring(1, line.length() - 1).split(",");

        ArrayList<String> res = new ArrayList<>();
        for (String s : arr) {
            s = s.trim();
            if (!s.isEmpty()) {
                res.add(s);
            }
        }

        return res.toArray(new String[res.size()]);
    }

    private static String readString(String line, BufferedReader reader) throws Exception {
        line = readValueLine(line);
        String delim = "";
        if (line.startsWith("\"") || line.startsWith("'")) {
            delim = line.substring(0, 1);
            if (line.startsWith("\"\"\"") || line.startsWith("'''")) {
                delim = line.substring(0, 3);
            }
        }

        if (StringUtil.isNullOrEmpty(delim)) {
            throw new IllegalArgumentException("Invalid string format, should start with ' or \" at least: " + line);
        }

        // Trim start delimiter
        if (line.startsWith(delim)) {
            line = line.substring(delim.length());
        }

        // Read multiline string
        if (!line.endsWith(delim)) {
            String s;
            while ((s = reader.readLine()) != null) {
                line += " " + s.trim();
                if (line.endsWith(delim)) {
                    break;
                }
            }
        }

        // Trim end delimiter
        if (line.endsWith(delim)) {
            line = line.substring(0, line.length() - delim.length());
        }

        return line.trim();
    }

    private static void validateAndAddSpec(List<EqlSpec> specs, EqlSpec spec, boolean supported) throws Exception {
        if (StringUtil.isNullOrEmpty(spec.query)) {
            throw new IllegalArgumentException("Read a test without a query value");
        }

        if (supported && spec.expectedEventIds == null) {
            throw new IllegalArgumentException("Read a test without a expected_event_ids value");
        }

        specs.add(spec);
    }

    // Simple .toml spec parsing of the original EQL spec
    // to avoid adding dependency on any actual toml library for now.
    private static List<EqlSpec> readFromStream(InputStream is, boolean supported) throws Exception {
        Map<String, Integer> testNames = new LinkedHashMap<>();
        List<EqlSpec> testSpecs = new ArrayList<>();

        EqlSpec spec = null;
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(is, StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // Ignore empty lines and comments
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                if (line.startsWith("[[queries]]")) {
                    if (spec != null) {
                        validateAndAddSpec(testSpecs, spec, supported);
                        spec = null;
                    }
                    spec = new EqlSpec();
                    continue;
                }
                if (line.startsWith("query")) {
                    spec.query = readString(line, reader);
                }

                if (line.startsWith("expected_event_ids")) {
                    spec.expectedEventIds = readExpectedEventIds(line, reader);
                }

                if (line.startsWith("tags")) {
                    spec.tags = readTags(line);
                }
                if (line.startsWith("note")) {
                    spec.note = readString(line, reader);
                }
                if (line.startsWith("description")) {
                    spec.description = readString(line, reader);
                }
            }
            // Append the last spec
            if (spec != null) {
                validateAndAddSpec(testSpecs, spec, supported);
            }
        }

        return testSpecs;
    }
}
