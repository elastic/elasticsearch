/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * A {@code {...}} argument is where a credential arrives, so its own {@code toString} has to render the keys and
 * nothing else — and the keys sorted, or the same bad input reads differently from one run to the next.
 */
public class TableFunctionArgumentTests extends ESTestCase {

    private static final String SECRET = "AKIAIOSFODNN7EXAMPLE";

    public void testNamedValuesRendersItsKeysSortedAndNoValues() {
        TableFunctionArgument.NamedValues values = new TableFunctionArgument.NamedValues(
            Source.EMPTY,
            Map.of("secret_key", SECRET, "access_key", SECRET, "format", "parquet")
        );

        assertThat(values.toString(), equalTo("NamedValues[access_key, format, secret_key]"));
        assertThat(values.toString(), not(containsString(SECRET)));
    }

    /** The written order is what a diagnostic listing keys reports, so the copy has to keep it. */
    public void testNamedValuesKeepsTheWrittenOrderOfItsKeys() {
        Map<String, Object> written = new LinkedHashMap<>();
        written.put("resource", "s3://bucket/data");
        written.put("format", "parquet");
        written.put("access_key", SECRET);

        TableFunctionArgument.NamedValues values = new TableFunctionArgument.NamedValues(Source.EMPTY, written);

        assertThat(values.values().keySet(), contains("resource", "format", "access_key"));
    }
}
