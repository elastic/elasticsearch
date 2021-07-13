/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import io.ous.jtoml.JToml;
import io.ous.jtoml.Toml;
import io.ous.jtoml.TomlTable;

import org.elasticsearch.common.Strings;

import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class EqlSpecLoader {

    private static void validateAndAddSpec(Set<EqlSpec> specs, EqlSpec spec) {
        if (Strings.isNullOrEmpty(spec.query())) {
            throw new IllegalArgumentException("Read a test without a query value");
        }

        if (specs.contains(spec)) {
            throw new IllegalArgumentException("Read a test query with the same queryNo");
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

    public static Collection<EqlSpec> readFromStream(InputStream is) throws Exception {
        Set<EqlSpec> testSpecs = new LinkedHashSet<>();

        EqlSpec spec;
        Toml toml = JToml.parse(is);

        List<TomlTable> queries = toml.getArrayTable("queries");
        for (TomlTable table : queries) {
            spec = new EqlSpec(table.getLong("queryNo").intValue());
            spec.seqCount(table.getLong("count"));
            List<?> arr = table.getList("expected_event_ids");
            if (arr != null) {
                long[] expectedEventIds = new long[arr.size()];
                int i = 0;
                for (Object obj : arr) {
                    expectedEventIds[i++] = (Long) obj;
                }
                spec.expectedEventIds(expectedEventIds);
            }

            arr = table.getList("filter_counts");
            if (arr != null) {
                long[] filterCounts = new long[arr.size()];
                int i = 0;
                for (Object obj : arr) {
                    filterCounts[i++] = (Long) obj;
                }
                spec.filterCounts(filterCounts);
            }

            arr = table.getList("filters");
            if (arr != null) {
                String[] filters = new String[arr.size()];
                int i = 0;
                for (Object obj : arr) {
                    filters[i++] = (String) obj;
                }
                spec.filters(filters);
            }

            spec.query(getTrimmedString(table, "query"));
            spec.time(table.getDouble("time"));

            validateAndAddSpec(testSpecs, spec);
        }

        return testSpecs;
    }
}
