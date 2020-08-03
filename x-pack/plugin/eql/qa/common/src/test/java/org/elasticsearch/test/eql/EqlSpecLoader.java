/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import io.ous.jtoml.JToml;
import io.ous.jtoml.Toml;
import io.ous.jtoml.TomlTable;

import org.elasticsearch.common.Strings;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class EqlSpecLoader {
    public static List<EqlSpec> load(String path, boolean supported, Set<String> uniqueTestNames) throws Exception {
        try (InputStream is = EqlSpecLoader.class.getResourceAsStream(path)) {
            return readFromStream(is, supported, uniqueTestNames);
        }
    }

    private static void validateAndAddSpec(List<EqlSpec> specs, EqlSpec spec, boolean supported,
        Set<String> uniqueTestNames) throws Exception {
        if (Strings.isNullOrEmpty(spec.name())) {
            throw new IllegalArgumentException("Read a test without a name value");
        }

        if (Strings.isNullOrEmpty(spec.query())) {
            throw new IllegalArgumentException("Read a test without a query value");
        }

        if (supported) {
            if (spec.expectedEventIds() == null) {
                throw new IllegalArgumentException("Read a test without a expected_event_ids value");
            }
            if (uniqueTestNames.contains(spec.name())) {
                throw new IllegalArgumentException("Found a test with the same name as another test: " + spec.name());
            } else {
                uniqueTestNames.add(spec.name());
            }
        }

        specs.add(spec);
    }

    private static String getTrimmedString(TomlTable table, String key) {
        String s = table.getString(key);
        if (s != null) {
            return s.trim();
        }
        return null;
    }

    private static List<EqlSpec> readFromStream(InputStream is, boolean supported, Set<String> uniqueTestNames) throws Exception {
        List<EqlSpec> testSpecs = new ArrayList<>();

        EqlSpec spec;
        Toml toml = JToml.parse(is);

        List<TomlTable> queries = toml.getArrayTable("queries");
        for (TomlTable table : queries) {
            spec = new EqlSpec();
            spec.query(getTrimmedString(table, "query"));
            spec.name(getTrimmedString(table, "name"));
            spec.note(getTrimmedString(table, "note"));
            spec.description(getTrimmedString(table, "description"));

            Boolean caseSensitive = table.getBoolean("case_sensitive");
            Boolean caseInsensitive = table.getBoolean("case_insensitive");
            // if case_sensitive is TRUE and case_insensitive is not TRUE (FALSE or NULL), then the test is case sensitive only
            if (Boolean.TRUE.equals(caseSensitive)) {
                if (Boolean.FALSE.equals(caseInsensitive) || caseInsensitive == null) {
                    spec.caseSensitive(true);
                }
            }
            // if case_sensitive is not TRUE (FALSE or NULL) and case_insensitive is TRUE, then the test is case insensitive only
            else if (Boolean.TRUE.equals(caseInsensitive)) {
                spec.caseSensitive(false);
            }
            // in all other cases, the test should run no matter the case sensitivity (should test both scenarios)

            List<?> arr = table.getList("tags");
            if (arr != null) {
                String tags[] = new String[arr.size()];
                int i = 0;
                for (Object obj : arr) {
                    tags[i] = (String) obj;
                }
                spec.tags(tags);
            }

            arr = table.getList("expected_event_ids");
            if (arr != null) {
                long expectedEventIds[] = new long[arr.size()];
                int i = 0;
                for (Object obj : arr) {
                    expectedEventIds[i++] = (Long) obj;
                }
                spec.expectedEventIds(expectedEventIds);
            }
            validateAndAddSpec(testSpecs, spec, supported, uniqueTestNames);
        }

        return testSpecs;
    }
}
