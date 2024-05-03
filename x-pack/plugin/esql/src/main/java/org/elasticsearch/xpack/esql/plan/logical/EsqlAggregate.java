/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * Extension of Aggregate for handling duplicates.
 * In ESQL is it possible to declare multiple aggregations and groupings with the same name, with the last declaration in grouping
 * winning.
 * As some of these declarations can be invalid, for validation reasons we need to keep the data around yet allowing will lead to
 * ambiguity in the output.
 * Hence this class - to allow the declaration to be moved over and thus for the Verifier to pick up the declaration while providing
 * a proper output.
 * To simplify things, the Aggregate class will be replaced with a vanilla one.
 */
public class EsqlAggregate extends Aggregate {

    private List<Attribute> lazyOutput;

    public EsqlAggregate(Source source, LogicalPlan child, List<Expression> groupings, List<? extends NamedExpression> aggregates) {
        super(source, child, groupings, aggregates);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(Expressions.asAttributes(aggregates()), emptyList());
        }

        return lazyOutput;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, EsqlAggregate::new, child(), groupings(), aggregates());
    }

    @Override
    public EsqlAggregate replaceChild(LogicalPlan newChild) {
        return new EsqlAggregate(source(), newChild, groupings(), aggregates());
    }
}
