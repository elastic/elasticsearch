/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.util.BytesRef;

import java.util.HashMap;
import java.util.Map;

/**
 * Accumulates the document-order leaf values (with inline nulls) of high-cardinality fields in strictly columnar index mode, keyed by the
 * binary doc-values field name. Unlike {@link FieldArrayContext}, this records NO sidecar {@code .offsets} field: the values live directly
 * in the field's own {@link MultiValuedBinaryDocValuesField.ArrayOrderInlineNull ArrayOrderInlineNull} binary doc-values column. Values are
 * accumulated here and flushed in {@link #addToLuceneDocument} so that all-null and empty-array documents (which write no binary blob)
 * still emit a {@code .counts} field.
 */
public class InlineBinaryArrayOrderContext {

    private final Map<String, MultiValuedBinaryDocValuesField.ArrayOrderInlineNull> valuesPerField = new HashMap<>();

    public void recordValue(String field, BytesRef value) {
        valuesPerField.computeIfAbsent(field, MultiValuedBinaryDocValuesField.ArrayOrderInlineNull::new).add(value);
    }

    public void recordNull(String field) {
        valuesPerField.computeIfAbsent(field, MultiValuedBinaryDocValuesField.ArrayOrderInlineNull::new).addNull();
    }

    public void recordEmptyArray(String field) {
        valuesPerField.computeIfAbsent(field, MultiValuedBinaryDocValuesField.ArrayOrderInlineNull::new);
    }

    public void addToLuceneDocument(DocumentParserContext context) {
        for (var entry : valuesPerField.entrySet()) {
            var field = entry.getValue();
            // The binary blob is written only when there is at least one non-null value; an all-null or empty array writes no blob and
            // is represented by the .counts field alone (see ArrayOrderInlineNull).
            if (field.hasNonNullValue()) {
                context.doc().add(field);
            }
            context.doc().add(NumericDocValuesField.indexedField(field.countFieldName(), field.count()));
        }
    }
}
