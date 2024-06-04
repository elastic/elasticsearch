/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Order;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.querydsl.container.Sort;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EsQueryExec extends LeafExec implements EstimatesRowSize {
    static final EsField DOC_ID_FIELD = new EsField("_doc", DataTypes.DOC_DATA_TYPE, Map.of(), false);
    static final EsField TSID_FIELD = new EsField("_tsid", DataTypes.TSID_DATA_TYPE, Map.of(), true);
    static final EsField TIMESTAMP_FIELD = new EsField("@timestamp", DataTypes.DATETIME, Map.of(), true);
    static final EsField INTERVAL_FIELD = new EsField("@timestamp_interval", DataTypes.DATETIME, Map.of(), true);

    private final EsIndex index;
    private final IndexMode indexMode;
    private final QueryBuilder query;
    private final Expression limit;
    private final List<FieldSort> sorts;
    private final List<Attribute> attrs;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    public record FieldSort(FieldAttribute field, Order.OrderDirection direction, Order.NullsPosition nulls) {
        public FieldSortBuilder fieldSortBuilder() {
            FieldSortBuilder builder = new FieldSortBuilder(field.name());
            builder.order(Sort.Direction.from(direction).asOrder());
            builder.missing(Sort.Missing.from(nulls).searchOrder());
            builder.unmappedType(field.dataType().esType());
            return builder;
        }
    }

    public EsQueryExec(Source source, EsIndex index, IndexMode indexMode, QueryBuilder query) {
        this(source, index, indexMode, sourceAttributes(source, indexMode), query, null, null, null);
    }

    public EsQueryExec(
        Source source,
        EsIndex index,
        IndexMode indexMode,
        List<Attribute> attrs,
        QueryBuilder query,
        Expression limit,
        List<FieldSort> sorts,
        Integer estimatedRowSize
    ) {
        super(source);
        this.index = index;
        this.indexMode = indexMode;
        this.query = query;
        this.attrs = attrs;
        this.limit = limit;
        this.sorts = sorts;
        this.estimatedRowSize = estimatedRowSize;
    }

    private static List<Attribute> sourceAttributes(Source source, IndexMode indexMode) {
        return switch (indexMode) {
            case STANDARD, LOGS -> List.of(new FieldAttribute(source, DOC_ID_FIELD.getName(), DOC_ID_FIELD));
            case TIME_SERIES -> List.of(
                new FieldAttribute(source, DOC_ID_FIELD.getName(), DOC_ID_FIELD),
                new FieldAttribute(source, TSID_FIELD.getName(), TSID_FIELD),
                new FieldAttribute(source, TIMESTAMP_FIELD.getName(), TIMESTAMP_FIELD),
                new FieldAttribute(source, INTERVAL_FIELD.getName(), INTERVAL_FIELD)
            );
        };
    }

    public static boolean isSourceAttribute(Attribute attr) {
        return DOC_ID_FIELD.getName().equals(attr.name());
    }

    @Override
    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(this, EsQueryExec::new, index, indexMode, attrs, query, limit, sorts, estimatedRowSize);
    }

    public EsIndex index() {
        return index;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    public QueryBuilder query() {
        return query;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    public Expression limit() {
        return limit;
    }

    public List<FieldSort> sorts() {
        return sorts;
    }

    public List<Attribute> attrs() {
        return attrs;
    }

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        int size;
        if (sorts == null || sorts.isEmpty()) {
            // track doc ids
            state.add(false, Integer.BYTES);
            size = state.consumeAllFields(false);
        } else {
            // track doc ids and segment ids
            state.add(false, Integer.BYTES * 2);
            size = state.consumeAllFields(true);
        }
        return Objects.equals(this.estimatedRowSize, size)
            ? this
            : new EsQueryExec(source(), index, indexMode, attrs, query, limit, sorts, size);
    }

    public EsQueryExec withLimit(Expression limit) {
        return Objects.equals(this.limit, limit)
            ? this
            : new EsQueryExec(source(), index, indexMode, attrs, query, limit, sorts, estimatedRowSize);
    }

    public boolean canPushSorts() {
        return indexMode != IndexMode.TIME_SERIES;
    }

    public EsQueryExec withSorts(List<FieldSort> sorts) {
        if (indexMode == IndexMode.TIME_SERIES) {
            assert false : "time-series index mode doesn't support sorts";
            throw new UnsupportedOperationException("time-series index mode doesn't support sorts");
        }
        return Objects.equals(this.sorts, sorts)
            ? this
            : new EsQueryExec(source(), index, indexMode, attrs, query, limit, sorts, estimatedRowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, indexMode, attrs, query, limit, sorts);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsQueryExec other = (EsQueryExec) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(indexMode, other.indexMode)
            && Objects.equals(attrs, other.attrs)
            && Objects.equals(query, other.query)
            && Objects.equals(limit, other.limit)
            && Objects.equals(sorts, other.sorts)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + index
            + "], "
            + "indexMode["
            + indexMode
            + "], "
            + "query["
            + (query != null ? Strings.toString(query, false, true) : "")
            + "]"
            + NodeUtils.limitedToString(attrs)
            + ", limit["
            + (limit != null ? limit.toString() : "")
            + "], sort["
            + (sorts != null ? sorts.toString() : "")
            + "] estimatedRowSize["
            + estimatedRowSize
            + "]";
    }
}
