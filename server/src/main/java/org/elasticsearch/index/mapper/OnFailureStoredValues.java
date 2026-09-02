/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Stores values that violated {@code doc_values.on_failure=ignore} (multi-value violations, and in strict-columnar index modes also
 * values that failed to parse with {@code ignore_malformed=true}) so indexing continues.
 * Each field gets its own sidecar column ({@link #ON_FAILURE_FIELD_NAME_SUFFIX}), read back by
 * {@link CompositeSyntheticFieldLoader#onFailureValuesLayer}; invisible to block loaders, ESQL, and aggregations.
 */
public final class OnFailureStoredValues {

    public static final String ON_FAILURE_FIELD_NAME_SUFFIX = "._on_failure";

    private OnFailureStoredValues() {}

    /**
     * Returns the name of the on-failure sidecar column for {@code fieldName}.
     */
    public static String name(String fieldName) {
        return fieldName + ON_FAILURE_FIELD_NAME_SUFFIX;
    }

    /**
     * Encodes the current parser value and stores it in the failure column for {@code fieldPath}, preserving encounter order and
     * duplicates so multiple violations on the same document are all retained.
     *
     * @param context the current document parsing context; the value is written to its Lucene document
     * @param fieldPath the full path of the field whose failure column the value is stored under
     * @param parser positioned at the value to store
     */
    public static void storeValueForOnFailureIgnore(DocumentParserContext context, String fieldPath, XContentParser parser)
        throws IOException {
        storeEncoded(context, fieldPath, XContentDataHelper.encodeToken(parser));
    }

    static void storeEncoded(DocumentParserContext context, String fieldPath, BytesRef encoded) {
        MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(
            context.doc(),
            name(fieldPath),
            encoded,
            MultiValuedBinaryDocValuesField.ValueOrdering.UNSORTED,
            context.indexSettings().getIndexVersionCreated()
        );
    }
}
