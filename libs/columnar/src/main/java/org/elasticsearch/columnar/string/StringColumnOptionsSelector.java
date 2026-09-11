/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.columnar.ColumnarFieldType;

/**
 * How a string column is written, asked once per field. What suits a field of a handful of repeated terms
 * is not what suits one whose values are nearly all distinct, and the field is what tells them apart.
 */
@FunctionalInterface
public interface StringColumnOptionsSelector {

    StringColumnOptions select(String fieldName, ColumnarFieldType type);

    /** Writes every field the same way. */
    static StringColumnOptionsSelector always(StringColumnOptions options) {
        return (fieldName, type) -> options;
    }
}
