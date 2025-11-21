/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EsQueryExec extends LeafExec implements EstimatesRowSize {
    public static final EsField DOC_ID_FIELD = new EsField(
        "_doc",
        DataType.DOC_DATA_TYPE,
        Map.of(),
        false,
        EsField.TimeSeriesFieldType.NONE
    );

    public static final List<EsField> TIME_SERIES_SOURCE_FIELDS = List.of(
        new EsField("_ts_slice_index", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE),
        new EsField("_ts_future_max_timestamp", DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
    );

    private final String indexPattern;
    private final IndexMode indexMode;
    private final List<Attribute> attrs;
    private final Expression limit;
    private final List<Sort> sorts;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final Integer estimatedRowSize;

    /**
     * queryBuilderAndTags may contain one or multiple {@code QueryBuilder}s, it is built on data node.
     * If there is one {@code RoundTo} function in the query plan, {@code ReplaceRoundToWithQueryAndTags} rule will build multiple
     * {@code QueryBuilder}s in the list, otherwise it is expected to contain only one {@code QueryBuilder} and its tag is
     * null.
     * It will be used by {@code EsPhysicalOperationProviders}.{@code sourcePhysicalOperation} to create
     * {@code LuceneSliceQueue.QueryAndTags}
     */
    private final List<QueryBuilderAndTags> queryBuilderAndTags;

    public interface Sort {
        SortBuilder<?> sortBuilder();

        Order.OrderDirection direction();

        FieldAttribute field();

        /**
         * Type of the <strong>result</strong> of the sort. For example,
         * geo distance will be {@link DataType#DOUBLE}.
         */
        DataType resulType();
    }

    public record FieldSort(FieldAttribute field, Order.OrderDirection direction, Order.NullsPosition nulls) implements Sort {
        @Override
        public SortBuilder<?> sortBuilder() {
            FieldSortBuilder builder = new FieldSortBuilder(field.name());
            builder.order(Direction.from(direction).asOrder());
            builder.missing(Missing.from(nulls).searchOrder());
            builder.unmappedType(field.dataType().esType());
            return builder;
        }

        @Override
        public DataType resulType() {
            return field.dataType();
        }
    }

    public record GeoDistanceSort(FieldAttribute field, Order.OrderDirection direction, double lat, double lon) implements Sort {
        @Override
        public SortBuilder<?> sortBuilder() {
            GeoDistanceSortBuilder builder = new GeoDistanceSortBuilder(field.name(), lat, lon);
            builder.order(Direction.from(direction).asOrder());
            return builder;
        }

        @Override
        public DataType resulType() {
            return DataType.DOUBLE;
        }
    }

    public record ScoreSort(Order.OrderDirection direction) implements Sort {
        @Override
        public SortBuilder<?> sortBuilder() {
            return new ScoreSortBuilder();
        }

        @Override
        public FieldAttribute field() {
            // TODO: refactor this: not all Sorts are backed by FieldAttributes
            return null;
        }

        @Override
        public DataType resulType() {
            return DataType.DOUBLE;
        }
    }

    public record QueryBuilderAndTags(QueryBuilder query, List<Object> tags) {
        @Override
        public String toString() {
            return "QueryBuilderAndTags{" + "queryBuilder=[" + query + "], tags=" + tags.toString() + "}";
        }
    };

    public EsQueryExec(
        Source source,
        String indexPattern,
        IndexMode indexMode,
        List<Attribute> attrs,
        Expression limit,
        List<Sort> sorts,
        Integer estimatedRowSize,
        List<QueryBuilderAndTags> queryBuilderAndTags
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.indexMode = indexMode;
        this.attrs = attrs;
        this.limit = limit;
        this.sorts = sorts;
        this.estimatedRowSize = estimatedRowSize;
        // cannot keep the ctor with QueryBuilder as it has the same number of arguments as this ctor, EsqlNodeSubclassTests will fail
        this.queryBuilderAndTags = queryBuilderAndTags;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public static boolean isDocAttribute(Attribute attr) {
        // While the user can create columns with the same name as DOC_ID_FIELD, they cannot create a field with the DOC_DATA_TYPE.
        return attr.typeResolved().resolved() && attr.dataType() == DataType.DOC_DATA_TYPE;
    }

    public boolean hasScoring() {
        for (Attribute a : attrs()) {
            if (a instanceof MetadataAttribute && a.name().equals(MetadataAttribute.SCORE)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(this, EsQueryExec::new, indexPattern, indexMode, attrs, limit, sorts, estimatedRowSize, queryBuilderAndTags);
    }

    public String indexPattern() {
        return indexPattern;
    }

    public IndexMode indexMode() {
        return indexMode;
    }

    /**
     * query is merged into queryBuilderAndTags, keep this method as it is called by many many places in both tests and product code.
     * If this method is called, the caller looks for the original queryBuilder, before {@code ReplaceRoundToWithQueryAndTags} converts it
     * to multiple queries with tags.
     */
    public QueryBuilder query() {
        return queryWithoutTag();
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    public Expression limit() {
        return limit;
    }

    public List<Sort> sorts() {
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
            : new EsQueryExec(source(), indexPattern, indexMode, attrs, limit, sorts, size, queryBuilderAndTags);
    }

    public EsQueryExec withLimit(Expression limit) {
        return Objects.equals(this.limit, limit)
            ? this
            : new EsQueryExec(source(), indexPattern, indexMode, attrs, limit, sorts, estimatedRowSize, queryBuilderAndTags);
    }

    public boolean canPushSorts() {
        return indexMode != IndexMode.TIME_SERIES;
    }

    public EsQueryExec withSorts(List<Sort> sorts) {
        if (indexMode == IndexMode.TIME_SERIES) {
            assert false : "time-series index mode doesn't support sorts";
            throw new UnsupportedOperationException("time-series index mode doesn't support sorts");
        }
        return Objects.equals(this.sorts, sorts)
            ? this
            : new EsQueryExec(source(), indexPattern, indexMode, attrs, limit, sorts, estimatedRowSize, queryBuilderAndTags);
    }

    /**
     * query is merged into queryBuilderAndTags, keep this method as it is called by too many places.
     * If this method is called, the caller looks for the original queryBuilder, before {@code ReplaceRoundToWithQueryAndTags} converts it
     * to multiple queries with tags.
     */
    public EsQueryExec withQuery(QueryBuilder query) {
        QueryBuilder thisQuery = queryWithoutTag();
        return Objects.equals(thisQuery, query)
            ? this
            : new EsQueryExec(
                source(),
                indexPattern,
                indexMode,
                attrs,
                limit,
                sorts,
                estimatedRowSize,
                List.of(new QueryBuilderAndTags(query, List.of()))
            );
    }

    public List<QueryBuilderAndTags> queryBuilderAndTags() {
        return queryBuilderAndTags;
    }

    public boolean canSubstituteRoundToWithQueryBuilderAndTags() {
        // LuceneTopNSourceOperator doesn't support QueryAndTags
        return sorts == null || sorts.isEmpty();
    }

    /**
     * Returns the original queryBuilder before {@code ReplaceRoundToWithQueryAndTags} converts it to multiple queryBuilder with tags.
     * If we reach here, the caller is looking for the original query before the rule converts it. If there are multiple queries in
     * queryBuilderAndTags or if the single query in queryBuilderAndTags already has a tag, that means
     * {@code ReplaceRoundToWithQueryAndTags} already applied to the original query, the original query cannot be retrieved any more,
     * exception will be thrown.
     */
    private QueryBuilder queryWithoutTag() {
        QueryBuilder queryWithoutTag;
        if (queryBuilderAndTags == null || queryBuilderAndTags.isEmpty()) {
            return null;
        } else if (queryBuilderAndTags.size() == 1) {
            QueryBuilderAndTags firstQuery = this.queryBuilderAndTags.get(0);
            if (firstQuery.tags().isEmpty()) {
                queryWithoutTag = firstQuery.query();
            } else {
                throw new UnsupportedOperationException("query is converted to query with tags: " + "[" + firstQuery + "]");
            }
        } else {
            throw new UnsupportedOperationException(
                "query is converted to multiple queries and tags: " + "[" + this.queryBuilderAndTags + "]"
            );
        }
        return queryWithoutTag;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, indexMode, attrs, limit, sorts, queryBuilderAndTags);
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
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(indexMode, other.indexMode)
            && Objects.equals(attrs, other.attrs)
            && Objects.equals(limit, other.limit)
            && Objects.equals(sorts, other.sorts)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(queryBuilderAndTags, other.queryBuilderAndTags);
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + indexPattern
            + "], "
            + "indexMode["
            + indexMode
            + "], "
            + NodeUtils.limitedToString(attrs)
            + ", limit["
            + (limit != null ? limit.toString() : "")
            + "], sort["
            + (sorts != null ? sorts.toString() : "")
            + "] estimatedRowSize["
            + estimatedRowSize
            + "] queryBuilderAndTags ["
            + (queryBuilderAndTags != null ? queryBuilderAndTags.toString() : "")
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
