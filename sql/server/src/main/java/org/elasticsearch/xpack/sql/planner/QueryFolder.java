/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.NestedFieldAttribute;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.FilterExec;
import org.elasticsearch.xpack.sql.plan.physical.LimitExec;
import org.elasticsearch.xpack.sql.plan.physical.OrderExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.sql.plan.physical.QuerylessExec;
import org.elasticsearch.xpack.sql.planner.QueryTranslator.GroupingContext;
import org.elasticsearch.xpack.sql.planner.QueryTranslator.QueryTranslation;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupingAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptSort;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.rule.Rule;
import org.elasticsearch.xpack.sql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xpack.sql.planner.QueryTranslator.and;
import static org.elasticsearch.xpack.sql.planner.QueryTranslator.toAgg;
import static org.elasticsearch.xpack.sql.planner.QueryTranslator.toQuery;
import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;

class QueryFolder extends RuleExecutor<PhysicalPlan> {
    PhysicalPlan fold(PhysicalPlan plan) {
        return execute(plan);
    }

    @Override
    protected Iterable<RuleExecutor<PhysicalPlan>.Batch> batches() {
        Batch rollup = new Batch("Fold queries",
                new FoldAggregate(),
                new FoldProject(),
                new FoldFilter(),
                new FoldOrderBy(), 
                new FoldLimit(),
                new FoldQueryless()
                );

        Batch finish = new Batch("Finish query", Limiter.ONCE,
                new PlanOutputToQueryRef()
                );

        return Arrays.asList(rollup, finish);
    }

    private static class FoldProject extends FoldingRule<ProjectExec> {

        @Override
        protected PhysicalPlan rule(ProjectExec project) {
            if (project.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) project.child();
                QueryContainer queryC = exec.queryContainer();
                
                Map<Attribute, Attribute> aliases = new LinkedHashMap<>(queryC.aliases());
                Map<Attribute, ColumnProcessor> processors = new LinkedHashMap<>(queryC.processors());
                
                for (NamedExpression pj : project.projections()) {
                    if (pj instanceof Alias) {
                        Attribute aliasAttr = pj.toAttribute();
                        Expression e = ((Alias) pj).child();

                        if (e instanceof ScalarFunction) {
                            aliases.put(aliasAttr, scalarToProcessor((ScalarFunction) e, processors));
                        }
                        else if (e instanceof NamedExpression) {
                            Attribute attr = ((NamedExpression) e).toAttribute();
                            aliases.put(aliasAttr, attr);
                        }
                        else {
                            throw new SqlIllegalArgumentException("Don't know how to translate expression %s", e);
                        }
                    }
                    else {
                        if (pj instanceof ScalarFunction) {
                            aliases.put(pj.toAttribute(), scalarToProcessor((ScalarFunction) pj, processors));
                        }
                        // field attribute / expression
                    }
                }

                QueryContainer clone = new QueryContainer(queryC.query(), queryC.aggs(), queryC.refs(), aliases, processors, queryC.pseudoFunctions(), queryC.sort(), queryC.limit());
                return new EsQueryExec(exec.location(), exec.index(), project.output(), clone);
            }
            return project;
        }

        private Attribute scalarToProcessor(ScalarFunction e, Map<Attribute, ColumnProcessor> processors) {
            List<Expression> trail = Functions.unwrapScalarFunctionWithTail(e);
            Expression tail = trail.get(trail.size() - 1);

            ColumnProcessor proc = Functions.chainProcessors(trail);

            // in projection, scalar functions can only be applied to constants (in which case they are folded) or columns aka NamedExpressions
            if (!(tail instanceof NamedExpression)) {
                throw new SqlIllegalArgumentException("Expected a NamedExpression but got %s", tail);
            }

            Attribute targetAttr = ((NamedExpression) tail).toAttribute();
            processors.put(targetAttr, proc);
            return targetAttr;
        }
    }

    private static class FoldFilter extends FoldingRule<FilterExec> {
        @Override
        protected PhysicalPlan rule(FilterExec plan) {

            if (plan.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) plan.child();
                QueryContainer qContainer = exec.queryContainer();

                QueryTranslation qt = toQuery(plan.condition(), plan.isHaving());
                
                Query query = (qContainer.query() != null || qt.query != null) ? and(plan.location(), qContainer.query(), qt.query) : null;
                Aggs aggs = addPipelineAggs(qContainer, qt);

                qContainer = new QueryContainer(query, aggs, qContainer.refs(), qContainer.aliases(), qContainer.processors(), qContainer.pseudoFunctions(), qContainer.sort(), qContainer.limit());

                return exec.with(qContainer);
            }
            return plan;
        }

        private Aggs addPipelineAggs(QueryContainer qContainer, QueryTranslation qt) {
            AggFilter filter = qt.aggFilter;
            Aggs aggs = qContainer.aggs();

            if (filter == null) {
                return qContainer.aggs();
            }

            // find the relevant groups and compute the shortest path (the highest group in the hierarchy)
            Map<String, GroupingAgg> groupPaths = new LinkedHashMap<>();
            // root group
            String shortestPath = null;
            GroupingAgg targetGroup = null;

            for (String refId : filter.aggRefs()) {
                // is it root group or agg property (_count)
                if (refId == null) {
                    shortestPath = StringUtils.EMPTY;
                }
                else {
                    // find function group
                    GroupingAgg groupAgg = qContainer.findGroupForAgg(refId);

                    if (groupAgg == null) {
                        groupAgg = qContainer.pseudoFunctions().get(refId);
                    }

                    if (groupAgg == null) {
                        throw new SqlIllegalArgumentException("Cannot find group for agg %s referrenced by agg filter %s(%s)", refId, filter.name(), filter);
                    }

                    String path = groupAgg.asParentPath();
                    if (shortestPath == null || shortestPath.length() > path.length()) {
                        shortestPath = path;
                        targetGroup = groupAgg;
                    }
                    groupPaths.put(refId, groupAgg);
                }
            }

            // and finally update the agg groups
            if (targetGroup == GroupingAgg.DEFAULT_GROUP) {
                throw new SqlIllegalArgumentException("Aggregation filtering not supported (yet) without explicit grouping");
                //aggs = aggs.addAgg(null, filter);
            }
            else {
                aggs = aggs.updateGroup(targetGroup.withPipelines(combine(targetGroup.subPipelines(), filter)));
            }

            return aggs;
        }
    }

    private static class FoldAggregate extends FoldingRule<AggregateExec> {
        @Override
        protected PhysicalPlan rule(AggregateExec a) {

            if (a.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) a.child();

                // build the group aggregation
                // and also collect info about it (since the group columns might be used inside the select)

                GroupingContext groupingContext = QueryTranslator.groupBy(a.groupings());
                // shortcut used in several places
                Map<ExpressionId, GroupingAgg> groupMap = groupingContext != null ? groupingContext.groupMap : emptyMap();

                QueryContainer queryC = exec.queryContainer();
                if (groupingContext != null) {
                    queryC = queryC.addGroups(groupingContext.groupMap.values());
                }

                Map<Attribute, Attribute> aliases = new LinkedHashMap<>();
                // tracker for compound aggs seen in a group
                Map<CompoundAggregate, String> compoundAggMap = new LinkedHashMap<>();

                // followed by actual aggregates
                for (NamedExpression ne : a.aggregates()) {
                    GroupingAgg parentGroup = null;
                    
                    // unwrap alias - it can be
                    // - an attribute (since we support aliases inside group-by)
                    // - a scalar function (used directly or acting on an argument already used for grouping)
                    // - an agg function (typically)

                    if (ne instanceof Alias || ne instanceof Function) {
                        Alias as = ne instanceof Alias ? (Alias) ne : null;
                        Expression child = as != null ? as.child() : ne;

                        // record aliases in case they are later referred in the tree
                        if (as != null) {
                            aliases.put(as.toAttribute(), ((NamedExpression) as.child()).toAttribute());
                        }

                        //
                        // look first for scalar functions which might wrap the actual grouped target
                        // (e.g. CAST(field) or ABS(field), etc..
                        //

                        List<Expression> wrappingFunctions = Functions.unwrapScalarFunctionWithTail(child);
                        ColumnProcessor proc = null;
                        Expression resolvedGroupedExp = null;
                        int resolvedExpIndex = -1;

                        // look-up the hierarchy to match the group
                        for (int i = wrappingFunctions.size() - 1; i >= 0 && resolvedGroupedExp == null; i--) {
                            Expression exp = wrappingFunctions.get(i);
                            parentGroup = groupingContext != null ? groupingContext.parentGroupFor(exp) : null;

                            // found group for expression or bumped into an aggregate (can happen when dealing with a root group)
                            if (parentGroup != null || Functions.isAggregateFunction(exp)) {
                                resolvedGroupedExp = exp;
                                resolvedExpIndex = i;
                            }
                        }
                        // if needed, combine the wrapping functions as processors
                        if (resolvedExpIndex >= 0) {
                            // sublist has the upper index exclusive hence the +1
                            proc = Functions.chainProcessors(wrappingFunctions.subList(0, resolvedExpIndex + 1));
                        }

                        // if there was no unwrapping, fallback to child
                        if (resolvedGroupedExp == null) {
                            resolvedGroupedExp = child;
                        }

                        // initialize parent if needed
                        parentGroup = parentGroup == null && groupingContext != null ? groupingContext.parentGroupFor(resolvedGroupedExp) : parentGroup;

                        if (resolvedGroupedExp instanceof Attribute) {
                            queryC = useNamedReference(((Attribute) resolvedGroupedExp), proc, groupMap, queryC);
                        }

                        // a scalar function can be used only if has been already used for grouping
                        // otherwise it is the opposite of grouping
                        else if (Functions.isScalarFunction(resolvedGroupedExp)) {
                            ScalarFunction sf = (ScalarFunction) resolvedGroupedExp;
                            
                            if (parentGroup == null) {
                                throw new SqlIllegalArgumentException("Scalar function %s can be used only if included already in grouping", sf.name());
                            }

                            queryC = queryC.addAggRef(parentGroup.propertyPath(), proc);
                            // redirect the alias to the scalar group id (changing the id altogether doesn't work since the agg path already uses it)
                            aliases.put(as.toAttribute(), sf.toAttribute().withId(sf.id()));
                        }
                        else {
                            if (!Functions.isAggregateFunction(resolvedGroupedExp)) {
                                throw new SqlIllegalArgumentException("Expected aggregate function inside alias; got %s", child.nodeString());
                            }
                            AggregateFunction f = (AggregateFunction) resolvedGroupedExp;
                            queryC = addFunction(parentGroup, f, proc, compoundAggMap, queryC);
                        }
                    }
                    // not an Alias, means it's an Attribute
                    else {
                        queryC = useNamedReference(ne, null, groupMap, queryC);
                    }
                }

                if (!aliases.isEmpty()) {
                    queryC = queryC.withAliases(combine(queryC.aliases(), aliases));
                }
                return new EsQueryExec(exec.location(), exec.index(), a.output(), queryC);
            }
            return a;
        }

        // the agg is an actual value (field) that points to a group 
        // so look it up and create an extractor for it
        private QueryContainer useNamedReference(NamedExpression ne, ColumnProcessor proc, Map<ExpressionId, GroupingAgg> groupMap, QueryContainer queryC) {
            GroupingAgg aggInfo = groupMap.get(ne.id());
            if (aggInfo == null) {
                throw new SqlIllegalArgumentException("Cannot find group '%s'", ne.name());
            }
            return queryC.addAggRef(aggInfo.propertyPath(), proc);
        }

        private QueryContainer addFunction(GroupingAgg parentAgg, AggregateFunction f, ColumnProcessor proc, Map<CompoundAggregate, String> compoundAggMap, QueryContainer queryC) {
            String functionId = f.functionId();
            // handle count as a special case agg
            if (f instanceof Count) {
                Count c = (Count) f;
                if (!c.distinct()) {
                    return queryC.addAggCount(parentAgg, functionId, proc);
                }
            }

            // otherwise translate it to an agg
            String parentPath = parentAgg != null ? parentAgg.asParentPath() : null;
            String groupId = parentAgg != null ? parentAgg.id() : null;

            if (f instanceof InnerAggregate) {
                InnerAggregate ia = (InnerAggregate) f;
                CompoundAggregate outer = ia.outer();
                String cAggPath = compoundAggMap.get(outer);

                // the compound agg hasn't been seen before so initialize it
                if (cAggPath == null) {
                    LeafAgg leafAgg = toAgg(parentPath, functionId, outer);
                    cAggPath = leafAgg.propertyPath();
                    compoundAggMap.put(outer, cAggPath);
                    // add the agg without the default ref to it
                    queryC = queryC.with(queryC.aggs().addAgg(leafAgg));
                }
                
                String aggPath = AggPath.metricValue(cAggPath, ia.innerId());
                // FIXME: concern leak - hack around MatrixAgg which is not generalized (afaik)
                if (ia.innerKey() != null) {
                    proc = QueryTranslator.matrixFieldExtractor(ia.innerKey()).andThen(proc);
                }

                return queryC.addAggRef(aggPath, proc);
            }

            return queryC.addAgg(groupId, toAgg(parentPath, functionId, f), proc);
        }
    }

    private static class FoldOrderBy extends FoldingRule<OrderExec> {
        @Override
        protected PhysicalPlan rule(OrderExec plan) {

            if (plan.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) plan.child();
                QueryContainer qContainer = exec.queryContainer();

                for (Order order : plan.order()) {
                    // check whether sorting is on an group (and thus nested agg) or field
                    Attribute attr = ((NamedExpression) order.child()).toAttribute();
                    // check whether there's an alias (occurs with scalar functions which are not named)
                    attr = qContainer.aliases().getOrDefault(attr, attr);
                    String lookup = attr.id().toString();
                    GroupingAgg group = qContainer.findGroupForAgg(lookup);

                    Direction direction = Direction.from(order.direction());

                    // TODO: might need to validate whether the target field or group actually exist
                    if (group != null && group != GroupingAgg.DEFAULT_GROUP) {
                        // check whether the lookup matches a group
                        if (group.id().equals(lookup)) {
                            qContainer = qContainer.updateGroup(group.with(direction));
                        }
                        // else it's a leafAgg
                        else {
                            qContainer = qContainer.updateGroup(group.with(lookup, direction));
                        }
                    }
                    else {
                        // scalar functions typically require script ordering
                        if (attr instanceof ScalarFunctionAttribute) {
                            ScalarFunctionAttribute sfa = (ScalarFunctionAttribute) attr;
                            // is there an expression to order by?
                            if (sfa.orderBy() != null) {
                                Expression ob = sfa.orderBy();
                                if (ob instanceof NamedExpression) {
                                    Attribute at = ((NamedExpression) ob).toAttribute();
                                    at = qContainer.aliases().getOrDefault(at, at);
                                    qContainer = qContainer.sort(new AttributeSort(at, direction));
                                }
                                // ignore constant 
                                else if (!ob.foldable()) {
                                    throw new SqlIllegalArgumentException("does not know how to order by expression %s", ob);
                                }
                            }
                            // nope, use scripted sorting
                            else {
                                qContainer = qContainer.sort(new ScriptSort(sfa.script(), direction));
                            }
                        }
                        else {
                            qContainer = qContainer.sort(new AttributeSort(attr, direction));
                        }
                    }
                }

                return exec.with(qContainer);
            }
            return plan;
        }
    }


    private static class FoldLimit extends FoldingRule<LimitExec> {

        @Override
        protected PhysicalPlan rule(LimitExec plan) {
            if (plan.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) plan.child();
                int limit = Integer.valueOf(QueryTranslator.valueOf(plan.limit()));
                int currentSize = exec.queryContainer().limit();
                int newSize = currentSize < 0 ? limit : Math.min(currentSize, limit); 
                return exec.with(exec.queryContainer().withLimit(newSize));
            }
            return plan;
        }
    }

    private static class FoldQueryless extends FoldingRule<PhysicalPlan> {

        @Override
        protected PhysicalPlan rule(PhysicalPlan plan) {
            if (plan.children().size() == 1 && plan.children().get(0) instanceof QuerylessExec) {
                return new QuerylessExec(plan.location(), new EmptyExecutable(plan.output()));
            }
            return plan;
        }
    }

    private static class PlanOutputToQueryRef extends FoldingRule<EsQueryExec> {
        @Override
        protected PhysicalPlan rule(EsQueryExec exec) {
            QueryContainer qContainer = exec.queryContainer();

            // references (aka aggs) are in place
            if (qContainer.hasReferences()) {
                return exec;
            }
                
            for (Attribute attr : exec.output()) {
                if (attr instanceof ScalarFunctionAttribute) {
                    attr = qContainer.aliases().get(attr);
                }
                if (attr instanceof RootFieldAttribute) {
                    qContainer = qContainer.addFieldRef((RootFieldAttribute) attr);
                }
                else if (attr instanceof NestedFieldAttribute) {
                    NestedFieldAttribute nfa = (NestedFieldAttribute) attr;
                    qContainer = qContainer.addNestedFieldRef(nfa);
                }
                else {
                    throw new SqlIllegalArgumentException("Unknown output attribute %s", attr);
                }
            }

            return exec.with(qContainer);
        }
    }

    // rule for folding physical plans together
    abstract static class FoldingRule<SubPlan extends PhysicalPlan> extends Rule<SubPlan, PhysicalPlan> {

        @Override
        public final PhysicalPlan apply(PhysicalPlan plan) {
            return plan.transformUp(this::rule, typeToken());
        }

        @Override
        protected abstract PhysicalPlan rule(SubPlan plan);
    }
}