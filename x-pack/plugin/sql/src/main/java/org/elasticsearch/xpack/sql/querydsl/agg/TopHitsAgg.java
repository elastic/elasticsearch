/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

    private final AggTarget sortTarget;
    private final SortOrder sortOrder;
    private final DataType fieldDataType;
    private final DataType sortFieldDataType;

    public TopHitsAgg(
        String id,
        AggTarget target,
        DataType fieldDataType,
        AggTarget sortTarget,
        DataType sortFieldDataType,
        SortOrder sortOrder
    ) {
        super(id, target);
        this.fieldDataType = fieldDataType;
        this.sortTarget = sortTarget;
        this.sortOrder = sortOrder;
        this.sortFieldDataType = sortFieldDataType;
    }

    @Override
    AggregationBuilder toBuilder() {
        // Sort missing values (NULLs) as last to get the first/last non-null value
        List<SortBuilder<?>> sortBuilderList = new ArrayList<>(2);
        if (sortTarget != null) {
            if (sortTarget.fieldName() != null) {
                sortBuilderList.add(
                    new FieldSortBuilder(sortTarget.fieldName()).order(sortOrder)
                        .missing(LAST.position())
                        .unmappedType(sortFieldDataType.esType())
                );
            } else if (sortTarget.script() != null) {
                sortBuilderList.add(
                    new ScriptSortBuilder(
                        Scripts.nullSafeSort(sortTarget.script()).toPainless(),
                        sortTarget.script().outputType().isNumeric()
                            ? ScriptSortBuilder.ScriptSortType.NUMBER
                            : ScriptSortBuilder.ScriptSortType.STRING
                    ).order(sortOrder)
                );
            }
        }

        if (target().fieldName() != null) {
            sortBuilderList.add(
                new FieldSortBuilder(target().fieldName()).order(sortOrder).missing(LAST.position()).unmappedType(fieldDataType.esType())
            );
        } else {
            sortBuilderList.add(
                new ScriptSortBuilder(
                    Scripts.nullSafeSort(target().script()).toPainless(),
                    target().script().outputType().isNumeric()
                        ? ScriptSortBuilder.ScriptSortType.NUMBER
                        : ScriptSortBuilder.ScriptSortType.STRING
                ).order(sortOrder)
            );
        }

        TopHitsAggregationBuilder builder = topHits(id());
        if (target().fieldName() != null) {
            return builder.docValueField(target().fieldName(), SqlDataTypes.format(fieldDataType)).sorts(sortBuilderList).size(1);
        } else {
            return builder.scriptField(id(), target().script().toPainless()).sorts(sortBuilderList).size(1);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortTarget, sortOrder, fieldDataType, sortFieldDataType);
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
        return Objects.equals(sortTarget, that.sortTarget) &&
                sortOrder==that.sortOrder &&
                Objects.equals(fieldDataType, that.fieldDataType) &&
                Objects.equals(sortFieldDataType, that.sortFieldDataType);
    }
}
