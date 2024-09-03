/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EsQueryExec extends LeafExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EsQueryExec",
        EsQueryExec::new
    );

    public static final EsField DOC_ID_FIELD = new EsField("_doc", DataType.DOC_DATA_TYPE, Map.of(), false);

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

    public record FieldSort(FieldAttribute field, Order.OrderDirection direction, Order.NullsPosition nulls) implements Writeable {
        public FieldSortBuilder fieldSortBuilder() {
            FieldSortBuilder builder = new FieldSortBuilder(field.name());
            builder.order(Direction.from(direction).asOrder());
            builder.missing(Missing.from(nulls).searchOrder());
            builder.unmappedType(field.dataType().esType());
            return builder;
        }

        private static FieldSort readFrom(StreamInput in) throws IOException {
            return new EsQueryExec.FieldSort(
                FieldAttribute.readFrom(in),
                in.readEnum(Order.OrderDirection.class),
                in.readEnum(Order.NullsPosition.class)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            field().writeTo(out);
            out.writeEnum(direction());
            out.writeEnum(nulls());
        }
    }

    public EsQueryExec(Source source, EsIndex index, IndexMode indexMode, List<Attribute> attributes, QueryBuilder query) {
        this(source, index, indexMode, attributes, query, null, null, null);
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

    private EsQueryExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            new EsIndex(in),
            EsRelation.readIndexMode(in),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalNamedWriteable(QueryBuilder.class),
            in.readOptionalNamedWriteable(Expression.class),
            in.readOptionalCollectionAsList(FieldSort::readFrom),
            in.readOptionalVInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        index().writeTo(out);
        EsRelation.writeIndexMode(out, indexMode());
        out.writeNamedWriteableCollection(output());
        out.writeOptionalNamedWriteable(query());
        out.writeOptionalNamedWriteable(limit());
        out.writeOptionalCollection(sorts());
        out.writeOptionalVInt(estimatedRowSize());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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

    public enum Direction {
        ASC,
        DESC;

        public static Direction from(Order.OrderDirection dir) {
            return dir == null || dir == Order.OrderDirection.ASC ? ASC : DESC;
        }

        public SortOrder asOrder() {
            return this == Direction.ASC ? SortOrder.ASC : SortOrder.DESC;
        }
    }

    public enum Missing {
        FIRST("_first"),
        LAST("_last"),
        /**
         * Nulls position has not been specified by the user and an appropriate default will be used.
         *
         * The default values are chosen such that it stays compatible with previous behavior. Unfortunately, this results in
         * inconsistencies across different types of queries (see https://github.com/elastic/elasticsearch/issues/77068).
         */
        ANY(null);

        private final String searchOrder;

        Missing(String searchOrder) {
            this.searchOrder = searchOrder;
        }

        public static Missing from(Order.NullsPosition pos) {
            return switch (pos) {
                case FIRST -> FIRST;
                case LAST -> LAST;
                default -> ANY;
            };
        }

        public String searchOrder() {
            return searchOrder;
        }
    }
}
