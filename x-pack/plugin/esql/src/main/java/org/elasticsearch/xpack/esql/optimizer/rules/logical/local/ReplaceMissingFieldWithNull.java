/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Look for any fields used in the plan that are missing locally and replace them with null.
 * This should minimize the plan execution, in the best scenario skipping its execution all together.
 */
public class ReplaceMissingFieldWithNull extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        AttributeSet lookupFields = new AttributeSet();
        plan.forEachUp(EsRelation.class, esRelation -> {
            if (esRelation.indexMode() == IndexMode.LOOKUP) {
                lookupFields.addAll(esRelation.output());
            }
        });

        return plan.transformUp(p -> missingToNull(p, localLogicalOptimizerContext.searchStats(), lookupFields));
    }

    private LogicalPlan missingToNull(LogicalPlan plan, SearchStats stats, AttributeSet lookupFields) {
        if (plan instanceof EsRelation || plan instanceof LocalRelation) {
            return plan;
        }

        if (plan instanceof Aggregate a) {
            // don't do anything (for now)
            return a;
        }
        // keep the aliased name
        else if (plan instanceof Project project) {
            var projections = project.projections();
            List<NamedExpression> newProjections = new ArrayList<>(projections.size());
            Map<DataType, Alias> nullLiteral = Maps.newLinkedHashMapWithExpectedSize(DataType.types().size());
            AttributeSet joinAttributes = joinAttributes(project);

            for (NamedExpression projection : projections) {
                // Do not use the attribute name, this can deviate from the field name for union types.
                if (projection instanceof FieldAttribute f
                    && stats.exists(f.fieldName()) == false
                    && joinAttributes.contains(f) == false
                    && f.field() instanceof PotentiallyUnmappedKeywordEsField == false) {
                    // TODO: Should do a searchStats lookup for join attributes instead of just ignoring them here
                    // See TransportSearchShardsAction
                    DataType dt = f.dataType();
                    Alias nullAlias = nullLiteral.get(f.dataType());
                    // save the first field as null (per datatype)
                    if (nullAlias == null) {
                        // In case of batch executions on data nodes and join exists, SearchStats may not always be available for all
                        // fields, creating a new alias for null with the same id as the field id can potentially cause planEval to add a
                        // duplicated ChannelSet to a layout, and Layout.builder().build() could throw a NullPointerException.
                        // As a workaround, assign a new alias id to the null alias when join exists and SearchStats is not available.
                        Alias alias = new Alias(f.source(), f.name(), Literal.of(f, null), joinAttributes.isEmpty() ? f.id() : null);
                        nullLiteral.put(dt, alias);
                        projection = alias.toAttribute();
                    }
                    // otherwise point to it
                    else {
                        // since avoids creating field copies
                        projection = new Alias(f.source(), f.name(), nullAlias.toAttribute(), f.id());
                    }
                }

                newProjections.add(projection);
            }
            // add the first found field as null
            if (nullLiteral.size() > 0) {
                plan = new Eval(project.source(), project.child(), new ArrayList<>(nullLiteral.values()));
                plan = new Project(project.source(), plan, newProjections);
            }
        } else if (plan instanceof Eval
            || plan instanceof Filter
            || plan instanceof OrderBy
            || plan instanceof RegexExtract
            || plan instanceof TopN) {
                plan = plan.transformExpressionsOnlyUp(
                    FieldAttribute.class,
                    // Do not use the attribute name, this can deviate from the field name for union types.
                    // Also skip fields from lookup indices because we do not have stats for these.
                    // TODO: We do have stats for lookup indices in case they are being used in the FROM clause; this can be refined.
                    f -> f.field() instanceof PotentiallyUnmappedKeywordEsField || (stats.exists(f.fieldName()) || lookupFields.contains(f))
                        ? f
                        : Literal.of(f, null)
                );
            } else if (plan instanceof MvExpand m) {
                NamedExpression target = m.target();
                AttributeSet joinAttributes = joinAttributes(m);
                if (joinAttributes.isEmpty() == false // rewrite only when there is join, TODO do we want to rewrite when there is no join?
                    && target instanceof FieldAttribute f
                    && stats.exists(f.fieldName()) == false
                    && joinAttributes.contains(f) == false
                    && f.field() instanceof PotentiallyUnmappedKeywordEsField == false) {
                    // Replace missing target field with null.
                    Alias alias = new Alias(f.source(), f.name(), Literal.of(f, null));
                    NamedExpression nullTarget = alias.toAttribute();
                    plan = new Eval(m.source(), m.child(), List.of(alias));
                    // The expanded reference is built on top of target field with the same name, and the parent plans all reference to the
                    // expanded reference other than the target field, keep expanded's id unchanged, otherwise the parent plans cannot find
                    // it.
                    Attribute nullExpanded = new ReferenceAttribute(
                        nullTarget.source(),
                        nullTarget.name(),
                        nullTarget.dataType(),
                        nullTarget.nullable(),
                        m.expanded().id(),
                        false
                    );
                    plan = new MvExpand(m.source(), plan, nullTarget, nullExpanded);
                }
            }
        return plan;
    }

    private AttributeSet joinAttributes(LogicalPlan plan) {
        var attributes = new AttributeSet();
        if (plan instanceof Project || plan instanceof MvExpand) {
            plan.forEachDown(Join.class, j -> j.right().forEachDown(EsRelation.class, p -> attributes.addAll(p.output())));
        }
        return attributes;
    }
}
