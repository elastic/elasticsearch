/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import io.ous.jtoml.JToml;
import io.ous.jtoml.Toml;
import io.ous.jtoml.TomlTable;

import org.elasticsearch.common.Strings;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EqlSpecLoader {

    public static List<EqlSpec> load(String path, Set<String> uniqueTestNames) throws Exception {
        try (InputStream is = EqlSpecLoader.class.getResourceAsStream(path)) {
            if (is == null) {
                throw new IllegalAccessException("Cannot find classpath resource " + path);
            }
            return readFromStream(is, uniqueTestNames);
        }
    }

    public static List<EqlSpec> load(String... paths) throws Exception {
        Set<String> uniqueTestNames = new HashSet<>();
        List<EqlSpec> specs = new ArrayList<>();
        for (String path : paths) {
            specs.addAll(load(path, uniqueTestNames));
        }
        return specs;
    }

    private static void validateAndAddSpec(List<EqlSpec> specs, EqlSpec spec, Set<String> uniqueTestNames) {
        if (Strings.isNullOrEmpty(spec.name())) {
            throw new IllegalArgumentException("Read a test without a name value");
        }

        if (Strings.isNullOrEmpty(spec.query())) {
            throw new IllegalArgumentException("Read a test without a query value");
        }

        if (spec.expectedEventIds() == null) {
            throw new IllegalArgumentException("Read a test without a expected_event_ids value");
        }
        if (uniqueTestNames.contains(spec.name())) { // TODO: scope it per file?
            throw new IllegalArgumentException("Found a test with the same name as another test: " + spec.name());
        } else {
            uniqueTestNames.add(spec.name());
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

    private static Integer getInteger(TomlTable table, String key) {
        Long s = table.getLong(key);
        if (s != null) {
            return s.intValue();
        }
        return null;
    }

    private static List<EqlSpec> readFromStream(InputStream is, Set<String> uniqueTestNames) throws Exception {
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
            spec.size(getInteger(table, "size"));

            List<?> arr = table.getList("tags");
            if (arr != null) {
                String[] tags = new String[arr.size()];
                int i = 0;
                for (Object obj : arr) {
                    tags[i++] = (String) obj;
                }
                spec.tags(tags);
            }

            arr = table.getList("expected_event_ids");
            if (arr != null) {
                List<long[]> expectedEventIdsList = new ArrayList<>();
                if (arr.size() == 0) {
                    expectedEventIdsList.add(new long[] {});
                } else if (arr.stream().allMatch(x -> x instanceof Long)) {
                    long[] expectedEventIds = asLongArray(arr);
                    expectedEventIdsList.add(expectedEventIds);
                } else if (arr.stream().allMatch(x -> x instanceof List)) {
                    for (Object o : arr) {
                        expectedEventIdsList.add(asLongArray((List) o));
                    }
                } else {
                    throw new IllegalArgumentException("Invalid expected_event_ids");
                }
                spec.expectedEventIds(expectedEventIdsList);
            }

            arr = table.getList("join_keys");
            spec.joinKeys(arr != null ? arr.toArray(new String[0]) : new String[0]);
            spec.maxSamplesPerKey(getInteger(table, "max_samples_per_key"));
            validateAndAddSpec(testSpecs, spec, uniqueTestNames);
        }

        return testSpecs;
    }

    private static long[] asLongArray(List<?> arr) {
        long[] expectedEventIds = new long[arr.size()];
        int i = 0;
        for (Object obj : arr) {
            expectedEventIds[i++] = (Long) obj;
        }
        return expectedEventIds;
    }
}
