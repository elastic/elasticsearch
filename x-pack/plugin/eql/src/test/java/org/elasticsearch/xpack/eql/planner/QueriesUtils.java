/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.planner;

import org.elasticsearch.common.Strings;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

final class QueriesUtils {

    private QueriesUtils() {}

    static Iterable<Object[]> readSpec(String url) throws Exception {
        ArrayList<Object[]> arr = new ArrayList<>();
        Map<String, Integer> testNames = new LinkedHashMap<>();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(QueryFolderOkTests.class.getResourceAsStream(url),
                StandardCharsets.UTF_8))) {
            int lineNumber = 0;
            String line;
            String name = null;
            String query = null;
            ArrayList<Object> expectations = new ArrayList<>(8);

            StringBuilder sb = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                lineNumber++;
                line = line.trim();

                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }

                if (name == null) {
                    name = line;
                    Integer previousName = testNames.put(name, lineNumber);
                    if (previousName != null) {
                        throw new IllegalArgumentException("Duplicate test name '" + line + "' at line " + lineNumber
                                + " (previously seen at line " + previousName + ")");
                    }
                }

                else if (query == null) {
                    sb.append(line);
                    if (line.endsWith(";")) {
                        sb.setLength(sb.length() - 1);
                        query = sb.toString();
                        sb.setLength(0);
                    }
                }

                else {
                    boolean done = false;
                    if (line.endsWith(";")) {
                        line = line.substring(0, line.length() - 1);
                        done = true;
                    }
                    // no expectation
                    if (line.equals("null") == false) {
                        expectations.add(line);
                    }

                    if (done) {
                        // Add and zero out for the next spec
                        addSpec(arr, name, query, expectations.isEmpty() ? null : expectations.toArray());
                        name = null;
                        query = null;
                        expectations.clear();
                    }
                }
            }

            if (name != null) {
                throw new IllegalStateException("Read a test [" + name + "] without a body at the end of [" + url + "]");
            }
        }
        return arr;
    }

    private static void addSpec(ArrayList<Object[]> arr, String name, String query, Object[] expectations) {
        if ((Strings.isNullOrEmpty(name) == false) && (Strings.isNullOrEmpty(query) == false)) {
            arr.add(new Object[] { name, query, expectations });
        }
    }
}
