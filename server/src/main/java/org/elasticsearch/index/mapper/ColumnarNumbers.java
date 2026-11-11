/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery;
import org.elasticsearch.columnar.ColumnarNumericRangeQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ColumnarLongIndexFieldData;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.index.mapper.blockloader.docvalues.ColumnarLongsBlockLoader;
import org.elasticsearch.script.field.LongDocValuesField;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

/**
 * How a number field whose doc values the ColumNAR codec stores is searched and read, see
 * {@link NumberFieldMapper.DocValuesDiskFormat#COLUMNAR}. Kept apart from {@link NumberType}, which serves the
 * {@code SORTED_NUMERIC} layout, with one constant per numeric type holding all of that type's readers.
 */
// TODO: add the other numeric types once ColumNAR stores them.
enum ColumnarNumbers {
    LONG {
        @Override
        Query termQuery(String field, Object value, IndexType indexType) {
            if (NumberType.hasDecimalPart(value)) {
                return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
            }
            if (NumberType.isOutOfLongRange(value)) {
                return Queries.newMatchNoDocsQuery("Value [" + value + "] is out of range");
            }
            long v = NumberType.LONG.parse(value, true).longValue();
            Query dvQuery = new ColumnarNumericRangeQuery(field, v, v);
            return indexType.hasPoints() ? new IndexOrDocValuesQuery(LongPoint.newExactQuery(field, v), dvQuery) : dvQuery;
        }

        @Override
        Query rangeQuery(
            String field,
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            IndexType indexType
        ) {
            return NumberType.longRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                Query dvQuery = new ColumnarNumericRangeQuery(field, l, u);
                if (indexType.hasTerms()) {
                    Query termsQuery = new TermRangeQuery(
                        field,
                        NumberFieldMapper.encodeLongIndexTerm(l),
                        NumberFieldMapper.encodeLongIndexTerm(u),
                        true,
                        true
                    );
                    return new IndexOrDocValuesQuery(termsQuery, dvQuery);
                }
                return indexType.hasPoints() ? new IndexOrDocValuesQuery(LongPoint.newRangeQuery(field, l, u), dvQuery) : dvQuery;
            });
        }

        @Override
        IndexFieldData.Builder fieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType) {
            return new ColumnarLongIndexFieldData.Builder(ft.name(), valuesSourceType, LongDocValuesField::new, ft.indexType());
        }

        @Override
        BlockLoader blockLoader(String field) {
            return new ColumnarLongsBlockLoader(field);
        }
    };

    /** The readers for {@code type}, which must be one the ColumNAR codec stores. */
    static ColumnarNumbers of(NumberType type) {
        return switch (type) {
            case LONG -> LONG;
            default -> throw new IllegalArgumentException("[" + type.typeName() + "] fields are not stored with the ColumNAR codec");
        };
    }

    /** A term query, matching the values the field indexed as points or terms where it has them, and its column otherwise. */
    abstract Query termQuery(String field, Object value, IndexType indexType);

    /** A range query, matching the values the field indexed as points or terms where it has them, and its column otherwise. */
    abstract Query rangeQuery(
        String field,
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        IndexType indexType
    );

    /** Field data for aggregations, sorting and scripts. */
    abstract IndexFieldData.Builder fieldDataBuilder(MappedFieldType ft, ValuesSourceType valuesSourceType);

    /** Loads the column into ES|QL blocks. */
    abstract BlockLoader blockLoader(String field);
}
