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
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.gen.script.Scripts;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.xpack.ql.querydsl.container.Sort.Missing.LAST;

public class TopHitsAgg extends LeafAgg {

    private final String sortField;
    private final ScriptTemplate sortScriptTemplate;
    private final SortOrder sortOrder;
    private final DataType fieldDataType;
    private final DataType sortFieldDataType;

    public TopHitsAgg(
        String id,
        Object fieldOrScript,
        DataType fieldDataType,
        Object sortFieldOrScript,
        DataType sortFieldDataType,
        SortOrder sortOrder
    ) {
        super(id, fieldOrScript);
        if (sortFieldOrScript == null) {
            this.sortField = null;
            this.sortScriptTemplate = null;
        } else if (sortFieldOrScript instanceof String) {
            this.sortField = (String) sortFieldOrScript;
            this.sortScriptTemplate = null;
        } else if (sortFieldOrScript instanceof ScriptTemplate) {
            this.sortField = null;
            this.sortScriptTemplate = (ScriptTemplate) sortFieldOrScript;
        } else {
            throw new SqlIllegalArgumentException("sorting argument of TopHits aggregation should be String or ScriptTemplate");
        }
        this.fieldDataType = fieldDataType;
        this.sortOrder = sortOrder;
        this.sortFieldDataType = sortFieldDataType;
    }

    @Override
    AggregationBuilder toBuilder() {
        // Sort missing values (NULLs) as last to get the first/last non-null value
        List<SortBuilder<?>> sortBuilderList = new ArrayList<>(2);
        if (sortField != null) {
            sortBuilderList.add(
                new FieldSortBuilder(sortField).order(sortOrder).missing(LAST.position()).unmappedType(sortFieldDataType.esType())
            );
        } else if (sortScriptTemplate != null) {
            sortBuilderList.add(
                new ScriptSortBuilder(
                    Scripts.nullSafeSort(sortScriptTemplate).toPainless(),
                    sortScriptTemplate.outputType().isNumeric()
                        ? ScriptSortBuilder.ScriptSortType.NUMBER
                        : ScriptSortBuilder.ScriptSortType.STRING
                ).order(sortOrder)
            );
        }

        if (fieldName() != null) {
            sortBuilderList.add(
                new FieldSortBuilder(fieldName()).order(sortOrder).missing(LAST.position()).unmappedType(fieldDataType.esType())
            );
        } else {
            sortBuilderList.add(
                new ScriptSortBuilder(
                    Scripts.nullSafeSort(scriptTemplate()).toPainless(),
                    scriptTemplate().outputType().isNumeric()
                        ? ScriptSortBuilder.ScriptSortType.NUMBER
                        : ScriptSortBuilder.ScriptSortType.STRING
                ).order(sortOrder)
            );
        }

        TopHitsAggregationBuilder builder = topHits(id());
        if (fieldName() != null) {
            return builder.docValueField(fieldName(), SqlDataTypes.format(fieldDataType)).sorts(sortBuilderList).size(1);
        } else {
            return builder.scriptField(id(), scriptTemplate().toPainless()).sorts(sortBuilderList).size(1);
        }
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
            && Objects.equals(sortScriptTemplate, that.sortScriptTemplate)
            && sortOrder == that.sortOrder
            && fieldDataType == that.fieldDataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sortField, sortScriptTemplate, sortOrder, fieldDataType);
    }
}
