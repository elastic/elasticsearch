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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.rule.ParameterizedRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Look for any fields used in the plan that are missing and replaces them with null or look for fields that are constant.
 * This should minimize the plan execution, in the best scenario skipping its execution all together.
 */
public class ReplaceFieldWithConstantOrNull extends ParameterizedRule<LogicalPlan, LogicalPlan, LocalLogicalOptimizerContext> {

    @Override
    public LogicalPlan apply(LogicalPlan plan, LocalLogicalOptimizerContext localLogicalOptimizerContext) {
        // Fields from lookup indices don't need to be present on the node, and our search stats don't include them, anyway. Ignore them.
        var lookupFieldsBuilder = AttributeSet.builder();
        Map<Attribute, Expression> attrToConstant = new HashMap<>();
        plan.forEachUp(EsRelation.class, esRelation -> {
            // Looking for indices in LOOKUP mode is correct: during parsing, we assign the expected mode and even if a lookup index
            // is used in the FROM command, it will not be marked with LOOKUP mode there - but STANDARD.
            // It seems like we could instead just look for JOINs and walk down their right hand side to find lookup fields - but this does
            // not work as this rule also gets called just on the right hand side of a JOIN, which means that we don't always know that
            // we're inside the right (or left) branch of a JOIN node. (See PlannerUtils.localPlan - this looks for FragmentExecs and
            // performs local logical optimization of the fragments; the right hand side of a LookupJoinExec can be a FragmentExec.)
            if (esRelation.indexMode() == IndexMode.LOOKUP) {
                lookupFieldsBuilder.addAll(esRelation.output());
            }
            // find constant values only in the main indices
            else if (esRelation.indexMode() == IndexMode.STANDARD) {
                for (Attribute attribute : esRelation.output()) {
                    if (attribute instanceof FieldAttribute fa) {
                        // Do not use the attribute name, this can deviate from the field name for union types; use fieldName() instead.
                        var val = localLogicalOptimizerContext.searchStats().constantValue(fa.fieldName());
                        if (val != null) {
                            attrToConstant.put(attribute, Literal.of(attribute, val));
                        }
                    }
                }
            }
        });

        // Do not use the attribute name, this can deviate from the field name for union types; use fieldName() instead.
        // Also retain fields from lookup indices because we do not have stats for these.
        Predicate<FieldAttribute> shouldBeRetained = f -> (localLogicalOptimizerContext.searchStats().exists(f.fieldName())
            || lookupFieldsBuilder.contains(f));

        return plan.transformUp(p -> replaceWithNullOrConstant(p, shouldBeRetained, attrToConstant));
    }

    private LogicalPlan replaceWithNullOrConstant(
        LogicalPlan plan,
        Predicate<FieldAttribute> shouldBeRetained,
        Map<Attribute, Expression> attrToConstant
    ) {
        if (plan instanceof EsRelation relation) {
            // For any missing field, place an Eval right after the EsRelation to assign null values to that attribute (using the same name
            // id!), thus avoiding that InsertFieldExtrations inserts a field extraction later.
            // This means that an EsRelation[field1, field2, field3] where field1 and field 3 are missing will be replaced by
            // Project[field1, field2, field3] <- keeps the ordering intact
            // \_Eval[field1 = null, field3 = null]
            // \_EsRelation[field1, field2, field3]
            List<Attribute> relationOutput = relation.output();
            Map<DataType, Alias> nullLiterals = Maps.newLinkedHashMapWithExpectedSize(DataType.types().size());
            List<NamedExpression> newProjections = new ArrayList<>(relationOutput.size());
            for (int i = 0, size = relationOutput.size(); i < size; i++) {
                Attribute attr = relationOutput.get(i);
                NamedExpression projection;
                if (attr instanceof FieldAttribute f && shouldBeRetained.test(f) == false) {
                    DataType dt = f.dataType();
                    Alias nullAlias = nullLiterals.get(dt);
                    // save the first field as null (per datatype)
                    if (nullAlias == null) {
                        // Keep the same id so downstream query plans don't need updating
                        // NOTE: THIS IS BRITTLE AND CAN LEAD TO BUGS.
                        // In case some optimizer rule or so inserts a plan node that requires the field BEFORE the Eval that we're adding
                        // on top of the EsRelation, this can trigger a field extraction in the physical optimizer phase, causing wrong
                        // layouts due to a duplicate name id.
                        // If someone reaches here AGAIN when debugging e.g. ClassCastExceptions NPEs from wrong layouts, we should probably
                        // give up on this approach and instead insert EvalExecs in InsertFieldExtraction.
                        Alias alias = new Alias(f.source(), f.name(), Literal.of(f, null), f.id());
                        nullLiterals.put(dt, alias);
                        projection = alias.toAttribute();
                    }
                    // otherwise point to it since this avoids creating field copies
                    else {
                        projection = new Alias(f.source(), f.name(), nullAlias.toAttribute(), f.id());
                    }
                } else {
                    projection = attr;
                }
                newProjections.add(projection);
            }

            if (nullLiterals.size() == 0) {
                return plan;
            }

            Eval eval = new Eval(plan.source(), relation, new ArrayList<>(nullLiterals.values()));
            // This projection is redundant if there's another projection downstream (and no commands depend on the order until we hit it).
            return new Project(plan.source(), eval, newProjections);
        }

        if (plan instanceof Eval
            || plan instanceof Filter
            || plan instanceof OrderBy
            || plan instanceof RegexExtract
            || plan instanceof TopN) {
            return plan.transformExpressionsOnlyUp(FieldAttribute.class, f -> {
                if (attrToConstant.containsKey(f)) {// handle constant values field and use the value itself instead
                    return attrToConstant.get(f);
                } else {// handle missing fields and replace them with null
                    return shouldBeRetained.test(f) ? f : Literal.of(f, null);
                }
            });
        }

        return plan;
    }
}
