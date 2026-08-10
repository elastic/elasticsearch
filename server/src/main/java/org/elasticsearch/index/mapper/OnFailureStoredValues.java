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
 * Stores the value that violated a strict {@code doc_values} constraint (currently just {@code multi_value=false}) when the field is
 * configured with {@code doc_values.on_failure=ignore}, so indexing can continue instead of rejecting the whole document.
 * <p>
 * Each field gets its own failure column, named by suffixing the field's full path with {@link #ON_FAILURE_FIELD_NAME_SUFFIX}, mirroring
 * how {@link IgnoreMalformedStoredValues} stores overflow from {@code ignore_above}/{@code ignore_malformed} - but kept as a separate
 * column and suffix, since a value redirected here is well-formed and simply violates a cardinality constraint, which is a different
 * failure reason than a malformed value.
 * <p>
 * On the read side the column is surfaced as a {@link CompositeSyntheticFieldLoader.Layer} via
 * {@link CompositeSyntheticFieldLoader#onFailureValuesLayer}, which appends the stored values after the primary column so that
 * {@code _source} reconstruction produces the original multi-valued array.
 * <p>
 * The column is deliberately <em>not</em> surfaced to block loaders, ESQL, or aggregations: {@code multi_value=false} advertises a
 * strictly single-valued column to those paths, and exposing the sidecar would break that contract.
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
