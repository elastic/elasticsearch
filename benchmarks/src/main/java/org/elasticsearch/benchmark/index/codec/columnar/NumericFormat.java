/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.ColumnarNumericRangeQuery;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.lucene.queries.SortedNumericDocValuesRangeQuery;

/**
 * A numeric doc-values format under comparison, abstracting the three parts that differ between the
 * TSDB codecs and ColumNAR: the codec, how a value is indexed, and the range query. This lets one
 * benchmark measure ColumNAR against the codecs it aims to reach parity with.
 */
public enum NumericFormat {

    ES819,
    ES95,
    COLUMNAR;

    private static final FieldType COLUMNAR_FIELD_TYPE = columnarFieldType();

    /** A codec that stores the numeric field with this format. */
    Codec codec() {
        final DocValuesFormat dv = switch (this) {
            case ES819 -> new ES819TSDBDocValuesFormat();
            case ES95 -> new ES95TSDBDocValuesFormat();
            case COLUMNAR -> new ColumNARDocValuesFormat();
        };
        return new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dv;
            }
        };
    }

    /** Adds {@code value} to {@code doc} in this format's on-disk shape (both carry a range skip index). */
    void addField(Document doc, String field, long value, BytesRefBuilder builder) {
        switch (this) {
            case ES819, ES95 -> doc.add(SortedNumericDocValuesField.indexedField(field, value));
            case COLUMNAR -> doc.add(
                new Field(field, BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { value }, 1, builder)), COLUMNAR_FIELD_TYPE)
            );
        }
    }

    /** This format's range query, both driving a skipper-aware doc-values range iterator. */
    Query rangeQuery(String field, long lower, long upper) {
        return switch (this) {
            case ES819, ES95 -> new SortedNumericDocValuesRangeQuery(field, lower, upper);
            case COLUMNAR -> new ColumnarNumericRangeQuery(field, lower, upper);
        };
    }

    private static FieldType columnarFieldType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, ColumnarFieldType.LONG.name());
        type.freeze();
        return type;
    }
}
