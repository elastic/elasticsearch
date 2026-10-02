/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.mapper.ColumnarBinaryDocValuesField;
import org.elasticsearch.sourcebatch.LuceneColumn;

/**
 * The doc-values column of a field whose values the ColumNAR codec stores, for a batch in which some documents hold no value.
 *
 * <p>Such a document carries the field through its payload alone, so it has to state the field's index options itself or Lucene
 * rejects it against the documents that do hold a value. On the row path
 * {@link ColumnarBinaryDocValuesField#fieldType()} answers that from the payload it is holding; a column has one type for every
 * document it covers, so the documents needing the other one are named here instead and given the same field the row path builds.
 *
 * <p>Only the {@link #rowFieldCursor() row cursor} differs. The column handed to {@code IndexWriter.addBatch} is the plain
 * doc-values column: a batch settles a field's index options on the column carrying the values, and Lucene rejects a second column
 * claiming the same for one field.
 */
public final class ColumnarPayloadColumn extends LuceneBinaryColumn {

    private final FixedBitSet valuelessDocs;
    private final IndexableFieldType typeWhenValueless;

    private ColumnarPayloadColumn(
        EscfColumn data,
        String name,
        IndexableFieldType fieldType,
        Density density,
        FixedBitSet filter,
        FixedBitSet valuelessDocs,
        IndexableFieldType typeWhenValueless
    ) {
        super(data, name, fieldType, density, filter);
        this.valuelessDocs = valuelessDocs;
        this.typeWhenValueless = typeWhenValueless;
    }

    /**
     * @param valuelessDocs      the documents whose payload holds no value, by batch-local doc id
     * @param typeWhenValueless  from {@link ColumnarBinaryDocValuesField#typeWhenValueless}
     */
    public static ColumnarPayloadColumn of(
        EscfColumnData data,
        String name,
        IndexableFieldType fieldType,
        FixedBitSet valuelessDocs,
        IndexableFieldType typeWhenValueless
    ) {
        final EscfColumn column = EscfColumn.from(data);
        return new ColumnarPayloadColumn(column, name, fieldType, densityOf(column), null, valuelessDocs, typeWhenValueless);
    }

    /** The type the {@link #rowFieldCursor() row cursor} gives {@code doc}, which is {@link #fieldType()} for all but the valueless. */
    public IndexableFieldType fieldTypeFor(int doc) {
        return valuelessDocs.get(doc) ? typeWhenValueless : fieldType();
    }

    @Override
    protected IndexableField fieldFor(int doc, BytesRef value) {
        // A plain Field cannot carry the valueless type over a BytesRef — it is tokenized, and that constructor rejects it — and
        // would be inverted as the single term its bytes spell. The payload field states the type and inverts into nothing.
        if (valuelessDocs.get(doc)) {
            return ColumnarBinaryDocValuesField.encoded(name(), value, false, typeWhenValueless);
        }
        return super.fieldFor(doc, value);
    }

    @Override
    public ColumnarPayloadColumn withFilter(FixedBitSet filter) {
        assert filter == null || filter.length() == data.docCount;
        return new ColumnarPayloadColumn(
            data,
            name(),
            fieldType(),
            densityOf(data),
            LuceneColumn.singleFilter(this.filter, filter),
            valuelessDocs,
            typeWhenValueless
        );
    }

    @Override
    public ColumnarPayloadColumn slice(int from, int count) {
        final EscfColumn sliced = data.sliceInternal(from, count);
        return new ColumnarPayloadColumn(
            sliced,
            name(),
            fieldType(),
            densityOf(sliced),
            EscfColumn.windowValidity(filter, from, count),
            EscfColumn.windowValidity(valuelessDocs, from, count),
            typeWhenValueless
        );
    }
}
