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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Similar to {@link EsQueryExec}, but this is a physical plan specifically for time series indices.
 */
public class TimeSeriesSourceExec extends LeafExec implements EstimatesRowSize {

    private final List<Attribute> attrs;
    private final QueryBuilder query;
    private final Expression limit;
    private final Integer estimatedRowSize;

    public TimeSeriesSourceExec(Source source, List<Attribute> attrs, QueryBuilder query, Expression limit, Integer estimatedRowSize) {
        super(source);
        this.attrs = attrs;
        this.query = query;
        this.limit = limit;
        this.estimatedRowSize = estimatedRowSize;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    protected NodeInfo<TimeSeriesSourceExec> info() {
        return NodeInfo.create(this, TimeSeriesSourceExec::new, attrs, query, limit, estimatedRowSize);
    }

    public QueryBuilder query() {
        return query;
    }

    public List<Attribute> attrs() {
        return attrs;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    public Expression limit() {
        return limit;
    }

    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        state.add(false, Integer.BYTES * 2);
        state.add(false, 22); // tsid
        state.add(false, 8); // timestamp
        int size = state.consumeAllFields(false);
        if (Objects.equals(this.estimatedRowSize, size)) {
            return this;
        } else {
            return new TimeSeriesSourceExec(source(), attrs, query, limit, size);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            attrs,
            query,

            limit,
            estimatedRowSize
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimeSeriesSourceExec other = (TimeSeriesSourceExec) obj;
        return Objects.equals(attrs, other.attrs)
            && Objects.equals(query, other.query)
            && Objects.equals(limit, other.limit)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + "query["
            + (query != null ? Strings.toString(query, false, true) : "")
            + "] attributes: ["
            + NodeUtils.limitedToString(attrs)
            + "], estimatedRowSize["
            + estimatedRowSize
            + "]";
    }
}
