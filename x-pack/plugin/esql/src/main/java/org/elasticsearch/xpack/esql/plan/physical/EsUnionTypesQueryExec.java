/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.type.MultiTypeEsField;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class EsUnionTypesQueryExec extends EsQueryExec {

    private final MultiTypeEsField multiTypeField;

    public EsUnionTypesQueryExec(Source source, EsIndex index, QueryBuilder query, MultiTypeEsField multiTypeField) {
        super(source, index, query);
        this.multiTypeField = multiTypeField;
    }

    public EsUnionTypesQueryExec(
        Source source,
        EsIndex index,
        List<Attribute> attrs,
        QueryBuilder query,
        Expression limit,
        List<FieldSort> sorts,
        Integer estimatedRowSize,
        MultiTypeEsField multiTypeField
    ) {
        super(source, index, attrs, query, limit, sorts, estimatedRowSize);
        this.multiTypeField = multiTypeField;
    }

    protected NodeInfo<EsQueryExec> info() {
        return NodeInfo.create(
            this,
            EsUnionTypesQueryExec::new,
            index(),
            attrs(),
            query(),
            limit(),
            sorts(),
            estimatedRowSize(),
            multiTypeField
        );
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
            : new EsUnionTypesQueryExec(source(), index(), attrs(), query(), limit(), sorts(), size, multiTypeField);
    }

    @Override
    public EsQueryExec withLimit(Expression limit) {
        return Objects.equals(this.limit(), limit)
            ? this
            : new EsUnionTypesQueryExec(source(), index(), attrs(), query(), limit, sorts(), estimatedRowSize(), multiTypeField);
    }

    @Override
    public EsQueryExec withSorts(List<FieldSort> sorts) {
        return Objects.equals(this.sorts(), sorts)
            ? this
            : new EsUnionTypesQueryExec(source(), index(), attrs(), query(), limit(), sorts, estimatedRowSize(), multiTypeField);
    }

    public MultiTypeEsField multiTypeField() {
        return multiTypeField;
    }
}
