/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for executing star-tree pre-aggregated queries.
 * <p>
 * This node represents a query that can be satisfied using pre-aggregated data
 * stored in a star-tree index, avoiding full document scans.
 */
public class EsStarTreeQueryExec extends LeafExec implements EstimatesRowSize {

    /**
     * Types of aggregations supported by star-tree.
     */
    public enum StarTreeAggType {
        SUM,
        COUNT,
        MIN,
        MAX,
        AVG
    }

    /**
     * Represents a star-tree aggregation request.
     */
    public record StarTreeAgg(
        String valueField,           // The value field to aggregate
        StarTreeAggType type,        // The aggregation type
        @Nullable String outputName, // The output attribute name
        boolean isLongField          // True if source field is integer/long type
    ) {
        @Override
        public String toString() {
            return type + "(" + valueField + ")" + (outputName != null ? " AS " + outputName : "");
        }
    }

    /**
     * Represents a grouping field filter for star-tree traversal.
     */
    public record GroupingFieldFilter(
        String groupingField,        // The grouping field name
        @Nullable Long ordinal,      // Specific ordinal to match (null = star/any)
        @Nullable String stringValue, // String value for keyword fields (resolved to ordinal at execution time)
        @Nullable List<String> stringValues, // List of string values for IN clause (resolved to ordinals at execution time)
        @Nullable Long rangeMin,     // Range filter minimum (inclusive)
        @Nullable Long rangeMax      // Range filter maximum (inclusive)
    ) {
        /**
         * Creates an exact match filter for a numeric ordinal.
         */
        public static GroupingFieldFilter exactMatch(String field, long ordinal) {
            return new GroupingFieldFilter(field, ordinal, null, null, null, null);
        }

        /**
         * Creates an exact match filter for a string value (ordinal resolved at execution time).
         */
        public static GroupingFieldFilter exactMatchString(String field, String value) {
            return new GroupingFieldFilter(field, null, value, null, null, null);
        }

        /**
         * Creates an IN clause filter for multiple string values (ordinals resolved at execution time).
         */
        public static GroupingFieldFilter inStrings(String field, List<String> values) {
            return new GroupingFieldFilter(field, null, null, List.copyOf(values), null, null);
        }

        /**
         * Creates a range filter.
         */
        public static GroupingFieldFilter range(String field, @Nullable Long min, @Nullable Long max) {
            return new GroupingFieldFilter(field, null, null, null, min, max);
        }

        public boolean isExactMatch() {
            return ordinal != null || stringValue != null;
        }

        public boolean isStringMatch() {
            return stringValue != null;
        }

        public boolean isInClause() {
            return stringValues != null && stringValues.isEmpty() == false;
        }

        public boolean isRange() {
            return rangeMin != null || rangeMax != null;
        }

        public boolean isStar() {
            return ordinal == null && stringValue == null && stringValues == null && rangeMin == null && rangeMax == null;
        }

        @Override
        public String toString() {
            if (ordinal != null) {
                return groupingField + "=" + ordinal;
            } else if (stringValue != null) {
                return groupingField + "=\"" + stringValue + "\"";
            } else if (stringValues != null) {
                return groupingField + " IN " + stringValues;
            } else if (isRange()) {
                return groupingField + " RANGE [" + rangeMin + ", " + rangeMax + "]";
            } else {
                return groupingField + "=*";
            }
        }
    }

    private final String indexPattern;
    private final String starTreeName;
    private final QueryBuilder query;
    private final Expression limit;
    private final List<Attribute> attrs;
    private final List<String> groupByFields;
    private final List<GroupingFieldFilter> groupingFieldFilters;
    private final List<StarTreeAgg> aggregations;
    private final AggregatorMode mode;

    public EsStarTreeQueryExec(
        Source source,
        String indexPattern,
        String starTreeName,
        @Nullable QueryBuilder query,
        @Nullable Expression limit,
        List<Attribute> attributes,
        List<String> groupByFields,
        List<GroupingFieldFilter> groupingFieldFilters,
        List<StarTreeAgg> aggregations,
        AggregatorMode mode
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.starTreeName = starTreeName;
        this.query = query;
        this.limit = limit;
        this.attrs = attributes;
        this.groupByFields = groupByFields;
        this.groupingFieldFilters = groupingFieldFilters;
        this.aggregations = aggregations;
        this.mode = mode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<EsStarTreeQueryExec> info() {
        return NodeInfo.create(
            this,
            EsStarTreeQueryExec::new,
            indexPattern,
            starTreeName,
            query,
            limit,
            attrs,
            groupByFields,
            groupingFieldFilters,
            aggregations,
            mode
        );
    }

    public String indexPattern() {
        return indexPattern;
    }

    public String starTreeName() {
        return starTreeName;
    }

    public @Nullable QueryBuilder query() {
        return query;
    }

    public @Nullable Expression limit() {
        return limit;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    public List<String> groupByFields() {
        return groupByFields;
    }

    public List<GroupingFieldFilter> groupingFieldFilters() {
        return groupingFieldFilters;
    }

    public List<StarTreeAgg> aggregations() {
        return aggregations;
    }

    public AggregatorMode mode() {
        return mode;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, attrs);
        state.consumeAllFields(false);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, starTreeName, query, limit, attrs, groupByFields, groupingFieldFilters, aggregations, mode);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EsStarTreeQueryExec other = (EsStarTreeQueryExec) obj;
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(starTreeName, other.starTreeName)
            && Objects.equals(query, other.query)
            && Objects.equals(limit, other.limit)
            && Objects.equals(attrs, other.attrs)
            && Objects.equals(groupByFields, other.groupByFields)
            && Objects.equals(groupingFieldFilters, other.groupingFieldFilters)
            && Objects.equals(aggregations, other.aggregations)
            && mode == other.mode;
    }

    @Override
    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append("[");
        sb.append(indexPattern);
        sb.append(", starTree=").append(starTreeName);
        sb.append(", mode=").append(mode);
        sb.append(", groupBy=").append(groupByFields);
        sb.append(", aggs=").append(aggregations);
        if (groupingFieldFilters.isEmpty() == false) {
            sb.append(", filters=").append(groupingFieldFilters);
        }
        if (query != null) {
            sb.append(", query=").append(Strings.toString(query, false, true));
        }
        sb.append("]");
        sb.append(NodeUtils.limitedToString(attrs));
        if (limit != null) {
            sb.append(", limit=").append(limit);
        }
        return sb.toString();
    }
}
