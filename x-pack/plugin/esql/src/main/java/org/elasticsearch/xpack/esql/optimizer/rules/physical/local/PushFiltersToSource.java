/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.Range;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.BinarySpatialFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushDownUtils.isAggregatable;

public class PushFiltersToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<FilterExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = filterExec;
        if (filterExec.child() instanceof EsQueryExec queryExec) {
            plan = planFilterExec(filterExec, queryExec, ctx);
        } else if (filterExec.child() instanceof EvalExec evalExec && evalExec.child() instanceof EsQueryExec queryExec) {
            plan = planFilterExec(filterExec, evalExec, queryExec, ctx);
        }
        return plan;
    }

    private static PhysicalPlan planFilterExec(FilterExec filterExec, EsQueryExec queryExec, LocalPhysicalOptimizerContext ctx) {
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : splitAnd(filterExec.condition())) {
            (canPushToSource(exp, x -> LucenePushDownUtils.hasIdenticalDelegate(x, ctx.searchStats())) ? pushable : nonPushable).add(exp);
        }
        return rewrite(filterExec, queryExec, pushable, nonPushable);
    }

    private static PhysicalPlan planFilterExec(
        FilterExec filterExec,
        EvalExec evalExec,
        EsQueryExec queryExec,
        LocalPhysicalOptimizerContext ctx
    ) {
        LinkedHashMap<NameId, FieldAttribute> refs = new LinkedHashMap<>();
        evalExec.fields().forEach(alias -> {
            if (alias.child() instanceof FieldAttribute fieldAttribute) {
                refs.put(alias.id(), fieldAttribute);
            } else if (alias.child() instanceof ReferenceAttribute ref && refs.containsKey(ref.id())) {
                FieldAttribute fieldAttribute = refs.get(ref.id());
                refs.put(alias.id(), fieldAttribute);
            }
        });
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : splitAnd(filterExec.condition())) {
            (canPushToSource(exp, refs, x -> LucenePushDownUtils.hasIdenticalDelegate(x, ctx.searchStats())) ? pushable : nonPushable).add(
                exp
            );
        }
        // Replace field references with their actual field attributes
        pushable.replaceAll(e -> e.transformDown(ReferenceAttribute.class, r -> fieldAttribute(r, refs)));
        return rewrite(filterExec, queryExec, pushable, nonPushable);
    }

    private static PhysicalPlan rewrite(
        FilterExec filterExec,
        EsQueryExec queryExec,
        List<Expression> pushable,
        List<Expression> nonPushable
    ) {
        // Combine GT, GTE, LT and LTE in pushable to Range if possible
        List<Expression> newPushable = combineEligiblePushableToRange(pushable);
        if (newPushable.size() > 0) { // update the executable with pushable conditions
            Query queryDSL = PlannerUtils.TRANSLATOR_HANDLER.asQuery(Predicates.combineAnd(newPushable));
            QueryBuilder planQuery = queryDSL.asBuilder();
            var query = Queries.combine(Queries.Clause.FILTER, asList(queryExec.query(), planQuery));
            queryExec = new EsQueryExec(
                queryExec.source(),
                queryExec.index(),
                queryExec.indexMode(),
                queryExec.output(),
                query,
                queryExec.limit(),
                queryExec.sorts(),
                queryExec.estimatedRowSize()
            );
            if (nonPushable.size() > 0) { // update filter with remaining non-pushable conditions
                return new FilterExec(filterExec.source(), queryExec, Predicates.combineAnd(nonPushable));
            } else { // prune Filter entirely
                return queryExec;
            }
        } // else: nothing changes
        return filterExec;
    }

    private static FieldAttribute fieldAttribute(Expression exp, Map<NameId, FieldAttribute> refs) {
        if (exp instanceof FieldAttribute fa) {
            return fa;
        } else if (exp instanceof ReferenceAttribute ra) {
            return refs.get(ra.id());
        }
        return null;
    }

    private static List<Expression> combineEligiblePushableToRange(List<Expression> pushable) {
        List<EsqlBinaryComparison> bcs = new ArrayList<>();
        List<Range> ranges = new ArrayList<>();
        List<Expression> others = new ArrayList<>();
        boolean changed = false;

        pushable.forEach(e -> {
            if (e instanceof GreaterThan || e instanceof GreaterThanOrEqual || e instanceof LessThan || e instanceof LessThanOrEqual) {
                if (((EsqlBinaryComparison) e).right().foldable()) {
                    bcs.add((EsqlBinaryComparison) e);
                } else {
                    others.add(e);
                }
            } else {
                others.add(e);
            }
        });

        for (int i = 0, step = 1; i < bcs.size() - 1; i += step, step = 1) {
            BinaryComparison main = bcs.get(i);
            for (int j = i + 1; j < bcs.size(); j++) {
                BinaryComparison other = bcs.get(j);
                if (main.left().semanticEquals(other.left())) {
                    // >/>= AND </<=
                    if ((main instanceof GreaterThan || main instanceof GreaterThanOrEqual)
                        && (other instanceof LessThan || other instanceof LessThanOrEqual)) {
                        bcs.remove(j);
                        bcs.remove(i);

                        ranges.add(
                            new Range(
                                main.source(),
                                main.left(),
                                main.right(),
                                main instanceof GreaterThanOrEqual,
                                other.right(),
                                other instanceof LessThanOrEqual,
                                main.zoneId()
                            )
                        );

                        changed = true;
                        step = 0;
                        break;
                    }
                    // </<= AND >/>=
                    else if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual)
                        && (main instanceof LessThan || main instanceof LessThanOrEqual)) {
                            bcs.remove(j);
                            bcs.remove(i);

                            ranges.add(
                                new Range(
                                    main.source(),
                                    main.left(),
                                    other.right(),
                                    other instanceof GreaterThanOrEqual,
                                    main.right(),
                                    main instanceof LessThanOrEqual,
                                    main.zoneId()
                                )
                            );

                            changed = true;
                            step = 0;
                            break;
                        }
                }
            }
        }
        return changed ? CollectionUtils.combine(others, bcs, ranges) : pushable;
    }

    public static boolean canPushToSource(Expression exp, Predicate<FieldAttribute> hasIdenticalDelegate) {
        return canPushToSource(exp, Map.of(), hasIdenticalDelegate);
    }

    public static boolean canPushToSource(
        Expression exp,
        Map<NameId, FieldAttribute> refs,
        Predicate<FieldAttribute> hasIdenticalDelegate
    ) {
        if (exp instanceof BinaryComparison bc) {
            return isAttributePushable(bc.left(), bc, refs, hasIdenticalDelegate) && bc.right().foldable();
        } else if (exp instanceof InsensitiveBinaryComparison bc) {
            return isAttributePushable(bc.left(), bc, refs, hasIdenticalDelegate) && bc.right().foldable();
        } else if (exp instanceof BinaryLogic bl) {
            return canPushToSource(bl.left(), refs, hasIdenticalDelegate) && canPushToSource(bl.right(), refs, hasIdenticalDelegate);
        } else if (exp instanceof In in) {
            return isAttributePushable(in.value(), null, refs, hasIdenticalDelegate) && Expressions.foldable(in.list());
        } else if (exp instanceof Not not) {
            return canPushToSource(not.field(), refs, hasIdenticalDelegate);
        } else if (exp instanceof UnaryScalarFunction usf) {
            if (usf instanceof RegexMatch<?> || usf instanceof IsNull || usf instanceof IsNotNull) {
                if (usf instanceof IsNull || usf instanceof IsNotNull) {
                    if (usf.field() instanceof FieldAttribute fa && fa.dataType().equals(DataType.TEXT)) {
                        return true;
                    }
                }
                return isAttributePushable(usf.field(), usf, refs, hasIdenticalDelegate);
            }
        } else if (exp instanceof CIDRMatch cidrMatch) {
            return isAttributePushable(cidrMatch.ipField(), cidrMatch, refs, hasIdenticalDelegate)
                && Expressions.foldable(cidrMatch.matches());
        } else if (exp instanceof SpatialRelatesFunction spatial) {
            return canPushSpatialFunctionToSource(spatial, refs);
        } else if (exp instanceof MatchQueryPredicate mqp) {
            return mqp.field() instanceof FieldAttribute && DataType.isString(mqp.field().dataType());
        } else if (exp instanceof StringQueryPredicate) {
            return true;
        } else if (exp instanceof FullTextFunction) {
            return true;
        }
        return false;
    }

    /**
     * Push-down to Lucene is only possible if one field is an indexed spatial field, and the other is a constant spatial or string column.
     */
    public static boolean canPushSpatialFunctionToSource(BinarySpatialFunction s, Map<NameId, FieldAttribute> refs) {
        // The use of foldable here instead of SpatialEvaluatorFieldKey.isConstant is intentional to match the behavior of the
        // Lucene pushdown code in EsqlTranslationHandler::SpatialRelatesTranslator
        // We could enhance both places to support ReferenceAttributes that refer to constants, but that is a larger change
        return isPushableSpatialAttribute(s.left(), refs) && s.right().foldable()
            || isPushableSpatialAttribute(s.right(), refs) && s.left().foldable();
    }

    private static boolean isPushableSpatialAttribute(Expression exp, Map<NameId, FieldAttribute> refs) {
        if (exp instanceof ReferenceAttribute ref && refs.containsKey(ref.id())) {
            return isPushableSpatialAttribute(refs.get(ref.id()));
        }
        return isPushableSpatialAttribute(exp);
    }

    private static boolean isPushableSpatialAttribute(Expression exp) {
        return exp instanceof FieldAttribute fa && fa.getExactInfo().hasExact() && isAggregatable(fa) && DataType.isSpatial(fa.dataType());
    }

    private static boolean isAttributePushable(
        Expression expression,
        Expression operation,
        Map<NameId, FieldAttribute> attributeMap,
        Predicate<FieldAttribute> hasIdenticalDelegate
    ) {
        if (expression instanceof ReferenceAttribute ref && attributeMap.containsKey(ref.id())) {
            return isAttributePushable(attributeMap.get(ref.id()), operation, hasIdenticalDelegate);
        }
        return isAttributePushable(expression, operation, hasIdenticalDelegate);
    }

    private static boolean isAttributePushable(
        Expression expression,
        Expression operation,
        Predicate<FieldAttribute> hasIdenticalDelegate
    ) {
        if (LucenePushDownUtils.isPushableFieldAttribute(expression, hasIdenticalDelegate)) {
            return true;
        }
        if (expression instanceof MetadataAttribute ma && ma.searchable()) {
            return operation == null
                // no range or regex queries supported with metadata fields
                || operation instanceof Equals
                || operation instanceof NotEquals
                || operation instanceof WildcardLike;
        }
        return false;
    }
}
