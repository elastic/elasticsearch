/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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

            if (fold != null) {
                List<TomlTable> tests = fold.getArrayTable("tests");

                for (TomlTable test : tests) {
                    String expression = getTrimmedString(test, "expression");
                    Object expected = test.get("expected");
                    EqlFoldSpec spec = new EqlFoldSpec(name, description, expression, expected);
                    testSpecs.add(spec);
                }
            }
        }

        return testSpecs;
    }
}
