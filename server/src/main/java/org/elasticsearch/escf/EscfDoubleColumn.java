/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.sourcebatch.SourceValueType;
import org.elasticsearch.xcontent.Text;

/**
 * An ESCF column whose values are all {@code double}s (JSON floats and doubles upcast to 64-bit raw bits). A value
 * whose spelling is not its canonical rendering carries that spelling in {@link #lexicalForms}; {@link #getStringValue}
 * answers with that spelling, and with the canonical {@code Double.toString} rendering for every other row, while
 * {@link #getDoubleValue} is unaffected (see {@link EscfColumnData#lexicalForms()}).
 */
final class EscfDoubleColumn extends AbstractFixed64Column {

    /** One row per document, zero-length where the canonical rendering is the spelling; {@code null} when no row has one. */
    @Nullable
    private final EscfStringColumn lexicalForms;

    EscfDoubleColumn(int docCount, FixedBitSet validity, BytesReference data) {
        this(docCount, validity, data, null);
    }

    EscfDoubleColumn(int docCount, FixedBitSet validity, BytesReference data, @Nullable EscfStringColumn lexicalForms) {
        super(docCount, validity, data);
        assert lexicalForms == null || lexicalForms.docCount() == docCount
            : "lexical forms have " + (lexicalForms == null ? 0 : lexicalForms.docCount()) + " rows, column has " + docCount;
        this.lexicalForms = lexicalForms;
    }

    @Override
    public byte kind() {
        return EscfColumnKind.DOUBLE;
    }

    @Override
    byte typeByteForPresent(int row) {
        return SourceValueType.DOUBLE;
    }

    @Override
    public double getDoubleValue(int row) {
        return Double.longBitsToDouble(rawLong(row));
    }

    @Override
    public Text getStringValue(int row) {
        if (hasLexicalForm(row) == false) {
            return new Text(Double.toString(getDoubleValue(row)));
        }
        return lexicalForms.getStringValue(row);
    }

    /** The stored spelling of the value at {@code row}, or {@code null} when its canonical rendering is its spelling. */
    @Nullable
    BytesRef lexicalForm(int row) {
        return hasLexicalForm(row) ? lexicalForms.getBinaryValue(row) : null;
    }

    private boolean hasLexicalForm(int row) {
        return lexicalForms != null && lexicalForms.valueLength(row) > 0;
    }

    @Override
    EscfColumn sliceInternal(int from, int count) {
        return new EscfDoubleColumn(
            count,
            windowValidity(validity, from, count),
            data.slice(from * 8, count * 8),
            lexicalForms == null ? null : (EscfStringColumn) lexicalForms.sliceInternal(from, count)
        );
    }

    @Override
    EscfColumnData toColumnData() {
        return EscfColumnData.ofDouble(docCount, validity, data, lexicalForms == null ? null : lexicalForms.toColumnData());
    }
}
