/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.xpack.sql.querydsl.container.Sort.Missing.LAST;

public class TopHitsAgg extends LeafAgg {

    private final String sortField;
    private final SortOrder sortOrder;
    private final DataType fieldDataType;
    private final DataType sortFieldDataType;


    public TopHitsAgg(String id, String fieldName, DataType fieldDataType, String sortField,
                      DataType sortFieldDataType, SortOrder sortOrder) {
        super(id, fieldName);
        this.sortField = sortField;
        this.sortOrder = sortOrder;
        this.fieldDataType = fieldDataType;
        this.sortFieldDataType = sortFieldDataType;
    }

    @Override
    AggregationBuilder toBuilder() {
        // Sort missing values (NULLs) as last to get the first/last non-null value
        List<SortBuilder<?>> sortBuilderList = new ArrayList<>(2);
        if (sortField != null) {
            sortBuilderList.add(
                new FieldSortBuilder(sortField)
                    .order(sortOrder)
                    .missing(LAST.position())
                    .unmappedType(sortFieldDataType.esType));
        }
        sortBuilderList.add(
                new FieldSortBuilder(fieldName())
                    .order(sortOrder)
                    .missing(LAST.position())
                    .unmappedType(fieldDataType.esType));

        return topHits(id()).docValueField(fieldName(), fieldDataType.format()).sorts(sortBuilderList).size(1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TopHitsAgg that = (TopHitsAgg) o;
        return Objects.equals(sortField, that.sortField)
            && sortOrder == that.sortOrder
            && fieldDataType == that.fieldDataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortField, sortOrder, fieldDataType);
    }
}
