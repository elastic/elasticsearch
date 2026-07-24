/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.builder;

import java.util.Locale;

/**
 * How a search treats field names that have no mapping, set by the {@code unmapped_fields} search body parameter. The name and values
 * mirror ES|QL's {@code SET unmapped_fields}.
 */
public enum UnmappedFields {
    /**
     * Unmapped names resolve to nothing, as they always have: queries match no documents, aggregations see no values.
     */
    DEFAULT,

    /**
     * Unmapped names absorbed by the index's {@code _unmapped} sink resolve to their values in the sink. A no-op on indices without a sink.
     */
    LOAD;

    public static UnmappedFields parse(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("[unmapped_fields] must be one of [default, load], but was [" + value + "]");
        }
    }
}
