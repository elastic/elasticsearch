/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.agg;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.xpack.ql.querydsl.container.Sort.Missing.LAST;

public class TopHitsAgg extends LeafAgg {

    private final AggSource sortSource;
    private final SortOrder sortOrder;
    private final DataType fieldDataType;
    private final DataType sortFieldDataType;

    public TopHitsAgg(
        String id,
        AggSource source,
        DataType fieldDataType,
        AggSource sortSource,
        DataType sortFieldDataType,
        SortOrder sortOrder
    ) {
        super(id, source);
        this.fieldDataType = fieldDataType;
        this.sortSource = sortSource;
        this.sortOrder = sortOrder;
        this.sortFieldDataType = sortFieldDataType;
    }

    @Override
    AggregationBuilder toBuilder() {
        // Sort missing values (NULLs) as last to get the first/last non-null value
        List<SortBuilder<?>> sortBuilderList = new ArrayList<>(2);
        if (sortSource!= null) {
            if (sortSource.fieldName() != null) {
                sortBuilderList.add(
                    new FieldSortBuilder(sortSource.fieldName()).order(sortOrder)
                        .missing(LAST.position())
                        .unmappedType(sortFieldDataType.esType())
                );
            } else if (sortSource.script() != null) {
                sortBuilderList.add(
                    new ScriptSortBuilder(
                        Scripts.nullSafeSort(sortSource.script()).toPainless(),
                        sortSource.script().outputType().isNumeric()
                            ? ScriptSortBuilder.ScriptSortType.NUMBER
                            : ScriptSortBuilder.ScriptSortType.STRING
                    ).order(sortOrder)
                );
            }
        }

        if (source().fieldName() != null) {
            sortBuilderList.add(
                new FieldSortBuilder(source().fieldName()).order(sortOrder).missing(LAST.position()).unmappedType(fieldDataType.esType())
            );
        } else {
            sortBuilderList.add(
                new ScriptSortBuilder(
                    Scripts.nullSafeSort(source().script()).toPainless(),
                    source().script().outputType().isNumeric()
                        ? ScriptSortBuilder.ScriptSortType.NUMBER
                        : ScriptSortBuilder.ScriptSortType.STRING
                ).order(sortOrder)
            );
        }

        TopHitsAggregationBuilder builder = topHits(id());
        if (source().fieldName() != null) {
            builder.docValueField(source().fieldName(), SqlDataTypes.format(fieldDataType));
        } else {
            builder.scriptField(id(), source().script().toPainless());
        }

        return builder.sorts(sortBuilderList).size(1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortSource, sortOrder, fieldDataType, sortFieldDataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        TopHitsAgg that = (TopHitsAgg) o;
        return Objects.equals(sortSource, that.sortSource) &&
                sortOrder==that.sortOrder &&
                Objects.equals(fieldDataType, that.fieldDataType) &&
                Objects.equals(sortFieldDataType, that.sortFieldDataType);
    }
}
