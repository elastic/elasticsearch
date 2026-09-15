/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node that pushes a geo-grid aggregation ({@code STATS COUNT(*) BY ST_GEOHASH/GEOTILE/GEOHEX(field, prec)})
 * down to Lucene, bypassing the normal field-extraction and evaluation pipeline.
 * <p>
 * This is a local-only (data-node-only) plan node. It is never serialized across the wire.
 * The planner creates it in place of the combination of {@link AggregateExec} + {@link EvalExec} + {@link EsQueryExec}
 * when all conditions for the pushdown are met.
 * <p>
 * Output columns mirror the intermediate aggregation attributes of the original {@link AggregateExec}:
 * {@code [cell_id (long), count (long), seen (boolean)]}.
 *
 * @see org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushGeoGridStatsToSource
 */
public class EsGeoGridAggQueryExec extends LeafExec implements EstimatesRowSize, DataSourceExec {

    private final String indexPattern;
    private final QueryBuilder query;
    private final String fieldName;
    private final int precision;
    private final DataType gridType;
    private final List<Attribute> attrs;

    public EsGeoGridAggQueryExec(
        Source source,
        String indexPattern,
        QueryBuilder query,
        String fieldName,
        int precision,
        DataType gridType,
        List<Attribute> attrs
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.query = query;
        this.fieldName = fieldName;
        this.precision = precision;
        this.gridType = gridType;
        this.attrs = attrs;
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
    protected NodeInfo<EsGeoGridAggQueryExec> info() {
        return NodeInfo.create(this, EsGeoGridAggQueryExec::new, indexPattern, query, fieldName, precision, gridType, attrs);
    }

    public String indexPattern() {
        return indexPattern;
    }

    public QueryBuilder query() {
        return query;
    }

    public String fieldName() {
        return fieldName;
    }

    public int precision() {
        return precision;
    }

    public DataType gridType() {
        return gridType;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, attrs);
        state.consumeAllFields(false);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern, query, fieldName, precision, gridType, attrs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EsGeoGridAggQueryExec other = (EsGeoGridAggQueryExec) obj;
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(query, other.query)
            && Objects.equals(fieldName, other.fieldName)
            && precision == other.precision
            && gridType == other.gridType
            && Objects.equals(attrs, other.attrs);
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName()).append('[').append(mapper.index(indexPattern)).append(']');
        sb.append(", gridType[").append(gridType.typeName()).append(']');
        sb.append(", field[").append(mapper.opaque(fieldName)).append(']');
        sb.append(", precision[").append(precision).append(']');
        sb.append(", query[").append(mapper.opaque(query != null ? Strings.toString(query, false, true) : "")).append(']');
        NodeUtils.toString(sb, attrs, format, mapper);
    }
}
