/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.FieldMapper.DocValuesParameter.Values.MultiValue;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering;

/**
 * Centralizes doc values field creation, branching between single-valued (multi_value=no) and multi-valued Lucene types.
 * <p>
 * For {@link MultiValue#NO}, uses single-valued Lucene types ({@link NumericDocValuesField}, {@link SortedDocValuesField},
 * {@link BinaryDocValuesField}) which enforce single-valuedness natively and enable storage optimizations.
 * For other multi-value modes, uses multi-valued types ({@link SortedNumericDocValuesField}, {@link SortedSetDocValuesField},
 * {@link MultiValuedBinaryDocValuesField}).
 */
public class DocValuesFieldFactory {

    private final MultiValue multiValue;
    private final boolean hasSkipper;
    private final IndexVersion indexVersion;

    public DocValuesFieldFactory(MultiValue multiValue, boolean hasSkipper, IndexVersion indexVersion) {
        this.multiValue = multiValue;
        this.hasSkipper = hasSkipper;
        this.indexVersion = indexVersion;
    }

    /**
     * Adds a numeric doc values field. For {@code multi_value=no}, creates a {@link NumericDocValuesField} (single-valued).
     * Otherwise, creates a {@link SortedNumericDocValuesField} (multi-valued).
     */
    public void addNumericField(LuceneDocument doc, String name, long value) {
        if (multiValue.isSingleValued()) {
            doc.add(hasSkipper ? NumericDocValuesField.indexedField(name, value) : new NumericDocValuesField(name, value));
        } else {
            doc.add(hasSkipper ? SortedNumericDocValuesField.indexedField(name, value) : new SortedNumericDocValuesField(name, value));
        }
    }

    /**
     * Adds a sorted (bytes) doc values field. For {@code multi_value=no}, creates a {@link SortedDocValuesField} (single-valued).
     * Otherwise, creates a {@link SortedSetDocValuesField} (multi-valued).
     */
    public void addSortedField(LuceneDocument doc, String name, BytesRef value) {
        if (multiValue.isSingleValued()) {
            doc.add(hasSkipper ? SortedDocValuesField.indexedField(name, value) : new SortedDocValuesField(name, value));
        } else {
            doc.add(hasSkipper ? SortedSetDocValuesField.indexedField(name, value) : new SortedSetDocValuesField(name, value));
        }
    }

    /**
     * Adds a binary doc values field. For {@code multi_value=no}, creates a plain {@link BinaryDocValuesField} (single-valued,
     * no companion {@code .counts} field). Otherwise, delegates to {@link MultiValuedBinaryDocValuesField#addToBinaryFieldInDoc}
     * which handles the multi-valued encoding.
     */
    public void addBinaryField(LuceneDocument doc, String name, BytesRef value, ValueOrdering ordering) {
        if (multiValue.isSingleValued()) {
            doc.add(new BinaryDocValuesField(name, value));
        } else {
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, name, value, ordering, indexVersion);
        }
    }
}
