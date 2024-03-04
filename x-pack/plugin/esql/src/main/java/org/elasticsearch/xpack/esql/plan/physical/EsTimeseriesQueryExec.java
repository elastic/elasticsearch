/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EsTimeseriesQueryExec extends EsQueryExec {

    static final EsField TSID_FIELD = new EsField("_tsid", DataTypes.KEYWORD, Map.of(), true);
    static final EsField TIMESTAMP_FIELD = new EsField("@timestamp", DataTypes.DATETIME, Map.of(), true);

    public EsTimeseriesQueryExec(Source source, EsIndex index, QueryBuilder query) {
        this(
            source,
            index,
            List.of(
                new FieldAttribute(source, DOC_ID_FIELD.getName(), DOC_ID_FIELD),
                new FieldAttribute(source, TSID_FIELD.getName(), TSID_FIELD),
                new FieldAttribute(source, TIMESTAMP_FIELD.getName(), TSID_FIELD)
            ),
            query,
            null,
            null,
            null
        );
    }

    public EsTimeseriesQueryExec(
        Source source,
        EsIndex index,
        List<Attribute> attrs,
        QueryBuilder query,
        Expression limit,
        List<FieldSort> sorts,
        Integer estimatedRowSize
    ) {
        super(source, index, attrs, query, limit, sorts, estimatedRowSize);
    }

    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(this, EsTimeseriesQueryExec::new, index(), attrs(), query(), limit(), sorts(), estimatedRowSize());
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        int size;
        if (sorts() == null || sorts().isEmpty()) {
            // track doc ids
            state.add(false, Integer.BYTES);
            size = state.consumeAllFields(false);
        } else {
            // track doc ids and segment ids
            state.add(false, Integer.BYTES * 2);
            size = state.consumeAllFields(true);
        }
        return Objects.equals(this.estimatedRowSize(), size)
            ? this
            : new EsTimeseriesQueryExec(source(), index(), attrs(), query(), limit(), sorts(), size);
    }

    @Override
    public EsQueryExec withLimit(Expression limit) {
        return Objects.equals(this.limit(), limit)
            ? this
            : new EsTimeseriesQueryExec(source(), index(), attrs(), query(), limit, sorts(), estimatedRowSize());
    }

    @Override
    public EsQueryExec withSorts(List<FieldSort> sorts) {
        return Objects.equals(this.sorts(), sorts)
            ? this
            : new EsTimeseriesQueryExec(source(), index(), attrs(), query(), limit(), sorts, estimatedRowSize());
    }
}
