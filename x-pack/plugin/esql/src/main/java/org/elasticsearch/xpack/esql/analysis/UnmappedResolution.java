/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Locale;

/**
 * This is an unmapped-fields strategy discriminator.
 */
public enum UnmappedResolution {
    /**
     * Use the default behavior for the query type: standard ESQL queries fail when referencing unmapped fields, while other query types
     * (e.g. PROMQL) may treat them differently.
     */
    DEFAULT,

    /**
     * In case the query references a field that's not present in the index mapping, alias this field to value {@code null} of type
     * {@link DataType#NULL}
     */
    NULLIFY,

    /**
     * In case the query references a field that's not present in the index mapping, attempt to load it from {@code _source}.
     */
    LOAD,

    /**
     * Load every source field that is not present in the index mapping, without requiring each unmapped field to be
     * referenced in the query. Each such field becomes its own {@code keyword} output column.
     */
    LOAD_ALL;

    /**
     * Whether unmapped fields are read from {@code _source}, regardless of how they are selected. Everything the two loading modes
     * share - loading the values, resolving referenced fields as keywords, tracking which indices lack a mapping - keys off this.
     */
    public boolean loadsUnmappedFields() {
        return this == LOAD || this == LOAD_ALL;
    }

    /**
     * Whether unmapped fields are read from {@code _source} without being referenced in the query, i.e. only {@link #LOAD_ALL}.
     */
    public boolean loadsAllUnmappedFields() {
        return this == LOAD_ALL;
    }

    /**
     * The value as it is spelled in {@code SET unmapped_fields=...}, for error messages that name the mode the user asked for.
     */
    public String settingValue() {
        return name().toLowerCase(Locale.ROOT);
    }
}
