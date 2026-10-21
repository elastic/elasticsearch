/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.nio.charset.StandardCharsets;

/** Option sets shared by the splitter suites, so two of them cannot drift apart while claiming the same dialect. */
final class CsvTestOptions {

    private CsvTestOptions() {}

    /** The grammar a plain {@code .csv} resolves to: comma-delimited, quoting on, escaping on. */
    static CsvFormatOptions csvDefaults() {
        return new CsvFormatOptions(
            ',',
            '"',
            '\\',
            "//",
            null,
            StandardCharsets.UTF_8,
            null,
            CsvFormatOptions.DEFAULT_MAX_FIELD_SIZE,
            CsvFormatOptions.MultiValueSyntax.NONE,
            true,
            CsvFormatOptions.DEFAULT_COLUMN_PREFIX,
            true,
            true,
            false
        );
    }
}
