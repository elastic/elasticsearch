/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.SeparateCount.COUNT_FIELD_SUFFIX;

public class HighCardinalityKeywordFieldDataTests extends AbstractStringFieldDataTestCase {

    @Override
    protected String getFieldDataType() {
        return "keyword_high_cardinality";
    }

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    @Override
    protected long minRamBytesUsed() {
        return 0;
    }

    // Trick to index multi-valued binary doc values fields using AbstractStringFieldDataTestCase:
    private Document current;
    private Map<String, MultiValuedBinaryDocValuesField> fields = new HashMap<>();

    @Override
    protected void addField(Document d, String name, String value) {
        if (current != d) {
            current = d;
            fields.clear();
        }

        d.add(new StringField(name, value, Field.Store.YES));
        var field = fields.get(name);
        var countsField = (NumericDocValuesField) d.getField(name + COUNT_FIELD_SUFFIX);
        if (field != null) {
            fields.get(name).add(new BytesRef(value));
            countsField.setLongValue(field.count());
        } else {
            field = new MultiValuedBinaryDocValuesField.SeparateCount(name, false);
            field.add(new BytesRef(value));
            countsField = NumericDocValuesField.indexedField(name + COUNT_FIELD_SUFFIX, 1);
            fields.put(name, field);
            d.add(field);
            d.add(countsField);
        }
    }

    @Override
    protected SortedBinaryDocValues.ValueMode expectedValueModeSingleValueAllSet() {
        return SortedBinaryDocValues.ValueMode.SINGLE_VALUED;
    }

    @Override
    protected SortedBinaryDocValues.Sparsity expectedSparsitySingleValueAllSet() {
        return SortedBinaryDocValues.Sparsity.DENSE;
    }

    @Override
    protected SortedBinaryDocValues.ValueMode expectedValueModeMultiValueAllSet() {
        return SortedBinaryDocValues.ValueMode.MULTI_VALUED;
    }

    @Override
    protected SortedBinaryDocValues.Sparsity expectedSparsityMultiValueAllSet() {
        return SortedBinaryDocValues.Sparsity.DENSE;
    }

    @Override
    protected SortedBinaryDocValues.ValueMode expectedValueModeMultiValueWithMissing() {
        return SortedBinaryDocValues.ValueMode.MULTI_VALUED;
    }

    @Override
    protected SortedBinaryDocValues.ValueMode expectedValueModeSingleValueWithMissing() {
        return SortedBinaryDocValues.ValueMode.SINGLE_VALUED;
    }

    @Override
    protected SortedBinaryDocValues.Sparsity expectedSparsityMultiValueWithMissing() {
        return SortedBinaryDocValues.Sparsity.SPARSE;
    }

    @Override
    protected SortedBinaryDocValues.Sparsity expectedSparsitySingleValueWithMissing() {
        return SortedBinaryDocValues.Sparsity.SPARSE;
    }

    // Don't run tests that binary doc values based field data doesn't support:
    public void testGlobalOrdinalsGetRemovedOnceIndexReaderCloses() throws Exception {
        assumeFalse("binary doc values don't support ordinals or global ordinals", true);
    }

    public void testSingleValuedGlobalOrdinals() throws Exception {
        assumeFalse("binary doc values don't support ordinals or global ordinals", true);
    }

    public void testGlobalOrdinals() throws Exception {
        assumeFalse("binary doc values don't support ordinals or global ordinals", true);
    }

    public void testTermsEnum() throws Exception {
        assumeFalse("binary doc values don't support terms enum", true);
    }
}
