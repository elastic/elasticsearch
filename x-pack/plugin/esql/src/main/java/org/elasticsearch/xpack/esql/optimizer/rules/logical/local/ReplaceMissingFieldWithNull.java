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
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
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

        AttributeMap<Attribute> fieldAttrReplacedBy = new AttributeMap<Attribute>();

        return plan.transformUp(p -> missingToNull(p, localLogicalOptimizerContext.searchStats(), lookupFields, fieldAttrReplacedBy));
    }

    private LogicalPlan missingToNull(
        LogicalPlan plan,
        SearchStats stats,
        AttributeSet lookupFields,
        AttributeMap<Attribute> fieldAttrReplacedBy
    ) {
        if (plan instanceof EsRelation || plan instanceof LocalRelation) {
            return plan;
        } else if (plan instanceof Project) {
            // Only create null literals for newly encountered missing fields
            plan = resolveAlreadyReplacedAttributes(plan, fieldAttrReplacedBy);
            Project project = (Project) plan;

            var projections = project.projections();
            List<NamedExpression> newProjections = new ArrayList<>(projections.size());
            Map<DataType, Alias> nullLiteral = Maps.newLinkedHashMapWithExpectedSize(DataType.types().size());
            AttributeSet joinAttributes = joinAttributes(project);

            for (NamedExpression projection : projections) {
                if (projection instanceof FieldAttribute f
                    // Do not use the attribute name, this can deviate from the field name for union types
                    && stats.exists(f.fieldName()) == false
                    && joinAttributes.contains(f) == false
                    && f.field() instanceof PotentiallyUnmappedKeywordEsField == false) {
                    // TODO: Should do a searchStats lookup for join attributes instead of just ignoring them here
                    // See TransportSearchShardsAction
                    DataType dt = f.dataType();
                    Alias nullAlias = nullLiteral.get(f.dataType());
                    // save the first field as null (per datatype)
                    if (nullAlias == null) {
                        Alias alias = new Alias(f.source(), f.name(), Literal.of(f, null), null);
                        nullLiteral.put(dt, alias);
                        projection = alias.toAttribute();
                    }
                    // otherwise point to it
                    else {
                        // since avoids creating field copies
                        projection = new Alias(f.source(), f.name(), nullAlias.toAttribute(), null);
                    }
                    fieldAttrReplacedBy.put(f, projection.toAttribute());
                }

                newProjections.add(projection);
            }

            // Add an EVAL ahead of the Project to create null literals
            if (nullLiteral.size() > 0) {
                plan = new Eval(project.source(), project.child(), new ArrayList<>(nullLiteral.values()));
                plan = new Project(project.source(), plan, newProjections);
            }

            return plan;
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

                return plan;
            }

        // If we replaced any field attribute by a null literal previously, we must reference the null literal now - the initial field
        // attribute is no longer available (projected away).
        return resolveAlreadyReplacedAttributes(plan, fieldAttrReplacedBy);
    }

    private LogicalPlan resolveAlreadyReplacedAttributes(LogicalPlan plan, AttributeMap<Attribute> fieldAttrReplacedBy) {
        if (fieldAttrReplacedBy.size() > 0) {
            plan = plan.transformExpressionsOnly(FieldAttribute.class, f -> {
                Attribute replacement = fieldAttrReplacedBy.get(f);
                return replacement == null ? f : replacement;
            });
        }
        return plan;
    }

    private AttributeSet joinAttributes(Project project) {
        var attributes = new AttributeSet();
        project.forEachDown(Join.class, j -> j.right().forEachDown(EsRelation.class, p -> attributes.addAll(p.output())));
        return attributes;
    }
}
