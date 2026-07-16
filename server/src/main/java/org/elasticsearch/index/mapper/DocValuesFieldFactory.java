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
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.ValueOrdering;

/**
 * Centralizes doc values field creation for fields that declare {@code multi_value=false} or {@code multi_value=true}.
 * <p>
 * Sortable numeric and bytes doc values are <em>always</em> written using the multi-valued Lucene types
 * ({@link SortedNumericDocValuesField}, {@link SortedSetDocValuesField}) regardless of the {@code multi_value} setting.
 * This ensures compatibility with index sorting: Lucene's index-sort machinery ({@code SortedNumericSortField},
 * {@code SortedSetSortField}) requires SORTED_NUMERIC / SORTED_SET doc values at segment-merge time.
 * <p>
 * Single-valuedness for {@code multi_value=false} fields is enforced at parse time via
 * {@link FieldMapper#isSingleValueEnforced()}, not by the Lucene doc-values type.
 * <p>
 * The {@code multi_value} flag only governs binary doc values ({@link BinaryDocValuesField} vs
 * {@link MultiValuedBinaryDocValuesField}), where the distinction is irrelevant to index sorting.
 */
public class DocValuesFieldFactory {

    private final boolean multiValue;
    private final boolean hasSkipper;
    private final IndexVersion indexVersion;

    public DocValuesFieldFactory(boolean multiValue, boolean hasSkipper, IndexVersion indexVersion) {
        this.multiValue = multiValue;
        this.hasSkipper = hasSkipper;
        this.indexVersion = indexVersion;
    }

    /**
     * The location of this is not-ideal, but it keeps things simple for now.
     * TODO: find a better place for this method.
     */
    public boolean isSingleValued() {
        return multiValue == false;
    }

    /**
     * Adds a numeric doc values field as {@link SortedNumericDocValuesField}. Always uses the multi-valued Lucene type
     * regardless of the {@code multi_value} setting, so that index-sort ({@code SortedNumericSortField}) works correctly
     * at segment-merge time for {@code multi_value=false} fields.
     */
    public void addNumericField(LuceneDocument doc, String name, long value) {
        doc.add(hasSkipper ? SortedNumericDocValuesField.indexedField(name, value) : new SortedNumericDocValuesField(name, value));
    }

    /**
     * Adds a sorted bytes doc values field as {@link SortedSetDocValuesField}. Always uses the multi-valued Lucene type
     * regardless of the {@code multi_value} setting, so that index-sort ({@code SortedSetSortField}) works correctly
     * at segment-merge time for {@code multi_value=false} fields.
     */
    public void addSortedField(LuceneDocument doc, String name, BytesRef value) {
        doc.add(hasSkipper ? SortedSetDocValuesField.indexedField(name, value) : new SortedSetDocValuesField(name, value));
    }

    /**
     * Adds a binary doc values field using the current index version's on-disk format. Use this overload for fields whose binary
     * doc values have always been written in {@link MultiValuedBinaryDocValuesField.SeparateCount SeparateCount} format.
     */
    public void addBinaryField(LuceneDocument doc, String name, BytesRef value, ValueOrdering ordering) {
        addBinaryField(doc, name, value, ordering, IndexVersion.current());
    }

    /**
     * Adds a binary doc values field using {@link #indexVersion} to select the on-disk encoding. Use this overload for fields that
     * historically stored {@link MultiValuedBinaryDocValuesField.IntegratedCount IntegratedCount} data
     * pre-{@link org.elasticsearch.index.IndexVersions#DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES}).
     */
    public void addBinaryFieldLegacyEncodingAware(LuceneDocument doc, String name, BytesRef value, ValueOrdering ordering) {
        addBinaryField(doc, name, value, ordering, indexVersion);
    }

    private void addBinaryField(LuceneDocument doc, String name, BytesRef value, ValueOrdering ordering, IndexVersion indexVersion) {
        if (isSingleValued()) {
            doc.add(new BinaryDocValuesField(name, value));
        } else {
            MultiValuedBinaryDocValuesField.addToBinaryFieldInDoc(doc, name, value, ordering, indexVersion);
        }
    }

}
