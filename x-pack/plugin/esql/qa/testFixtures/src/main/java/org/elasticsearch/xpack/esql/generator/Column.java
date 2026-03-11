/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.List;

/**
 * A column in the output schema of a query execution.
 * @param name the field name
 * @param type the field type
 * @param originalTypes the original types, in case of a union type.
 * @param indexMapped whether this column originates from an actual index mapping ({@code true}) or was
 *                    computed/derived by a pipeline command such as EVAL, GROK, DISSECT, ENRICH, etc. ({@code false}).
 *                    Full-text functions (match, match_phrase, multi_match, {@code :} operator) require index-mapped fields.
 *                    An exception here is MV_EXPAND which, even if has as ouput a field from an index, this field is forbidden to be used
 *                    in full-text functions, as well. See https://github.com/elastic/elasticsearch/issues/142713
 */
public record Column(String name, String type, List<String> originalTypes, boolean indexMapped) {
    /**
     * Backward-compatible constructor that defaults to {@code indexMapped = true}.
     */
    public Column(String name, String type, List<String> originalTypes) {
        this(name, type, originalTypes, true);
    }
}
