/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.optimizer;

import io.ous.jtoml.JToml;
import io.ous.jtoml.Toml;
import io.ous.jtoml.TomlTable;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


public class EqlFoldSpecLoader {
    public static List<EqlFoldSpec> load(String path) throws Exception {
        try (InputStream is = EqlFoldSpecLoader.class.getResourceAsStream(path)) {
            return readFromStream(is);
        }
    }

    private static String getTrimmedString(TomlTable table, String key) {
        String s = table.getString(key);
        if (s != null) {
            return s.trim();
        }
        return null;
    }

    private static List<EqlFoldSpec> readFromStream(InputStream is) throws Exception {
        List<EqlFoldSpec> testSpecs = new ArrayList<>();

        Toml toml = JToml.parse(is);

        for (String name : toml.keySet()) {
            TomlTable table = toml.getTomlTable(name);
            TomlTable fold = table.getTomlTable("fold");

            String description = getTrimmedString(table, "description");
            Boolean cs = null;
            Boolean caseSensitive = table.getBoolean("case_sensitive");
            Boolean caseInsensitive = table.getBoolean("case_insensitive");
            // if case_sensitive is TRUE and case_insensitive is not TRUE (FALSE or NULL), then the test is case sensitive only
            if (Boolean.TRUE.equals(caseSensitive)) {
                if (Boolean.FALSE.equals(caseInsensitive) || caseInsensitive == null) {
                    cs = true;
                }
            }
            // if case_sensitive is not TRUE (FALSE or NULL) and case_insensitive is TRUE, then the test is case insensitive only
            else if (Boolean.TRUE.equals(caseInsensitive)) {
                cs = false;
            }
            // in all other cases, the test should run no matter the case sensitivity (should test both scenarios)

            if (fold != null) {
                List<TomlTable> tests = fold.getArrayTable("tests");

                for (TomlTable test : tests) {
                    String expression = getTrimmedString(test, "expression");
                    Object expected = test.get("expected");
                    EqlFoldSpec spec = new EqlFoldSpec(name, description, cs, expression, expected);
                    testSpecs.add(spec);
                }
            }
        }

        return testSpecs;
    }
}
