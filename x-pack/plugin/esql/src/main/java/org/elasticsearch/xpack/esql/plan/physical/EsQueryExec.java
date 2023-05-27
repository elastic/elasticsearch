/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.querydsl.container.Sort;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.NodeUtils;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.List;
import java.util.Map;
import java.util.Objects;

@Experimental
public class EsQueryExec extends LeafExec {
    public static final DataType DOC_DATA_TYPE = new DataType("_doc", Integer.BYTES * 3, false, false, false);

    static final EsField DOC_ID_FIELD = new EsField("_doc", DOC_DATA_TYPE, Map.of(), false);

    public static boolean isSourceAttribute(Attribute attr) {
        return "_doc".equals(attr.name());
    }

    private final EsIndex index;
    private final QueryBuilder query;
    private final Expression limit;
    private final List<FieldSort> sorts;
    private final List<Attribute> attrs;

    public record FieldSort(FieldAttribute field, Order.OrderDirection direction, Order.NullsPosition nulls) {
        public FieldSortBuilder fieldSortBuilder() {
            FieldSortBuilder builder = new FieldSortBuilder(field.name());
            builder.order(Sort.Direction.from(direction).asOrder());
            builder.missing(Sort.Missing.from(nulls).searchOrder());
            builder.unmappedType(field.dataType().esType());
            return builder;
        }
    }

    public EsQueryExec(Source source, EsIndex index, QueryBuilder query) {
        this(source, index, List.of(new FieldAttribute(source, DOC_ID_FIELD.getName(), DOC_ID_FIELD)), query, null, null);
    }

    public EsQueryExec(Source source, EsIndex index, List<Attribute> attrs, QueryBuilder query, Expression limit, List<FieldSort> sorts) {
        super(source);
        this.index = index;
        this.query = query;
        this.attrs = attrs;
        this.limit = limit;
        this.sorts = sorts;
    }

    @Override
    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(this, EsQueryExec::new, index, attrs, query, limit, sorts);
    }

    public EsIndex index() {
        return index;
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

    public EsQueryExec withQuery(QueryBuilder query) {
        return Objects.equals(this.query, query) ? this : new EsQueryExec(source(), index, attrs, query, limit, sorts);
    }

    public EsQueryExec withLimit(Expression limit) {
        return Objects.equals(this.limit, limit) ? this : new EsQueryExec(source(), index, attrs, query, limit, sorts);
    }

    public EsQueryExec withSorts(List<FieldSort> sorts) {
        return Objects.equals(this.sorts, sorts) ? this : new EsQueryExec(source(), index, attrs, query, limit, sorts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, attrs, query, limit, sorts);
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
            && Objects.equals(attrs, other.attrs)
            && Objects.equals(query, other.query)
            && Objects.equals(limit, other.limit)
            && Objects.equals(sorts, other.sorts);
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + index
            + "], query["
            + (query != null ? Strings.toString(query, false, true) : "")
            + "]"
            + NodeUtils.limitedToString(attrs)
            + ", limit["
            + (limit != null ? limit.toString() : "")
            + "], sort["
            + (sorts != null ? sorts.toString() : "")
            + "]";
    }
}
