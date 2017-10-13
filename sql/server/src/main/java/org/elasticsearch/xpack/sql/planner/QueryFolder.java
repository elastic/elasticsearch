/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundNumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeHistogramFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.AggPathInput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.FilterExec;
import org.elasticsearch.xpack.sql.plan.physical.LimitExec;
import org.elasticsearch.xpack.sql.plan.physical.LocalExec;
import org.elasticsearch.xpack.sql.plan.physical.OrderExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.sql.planner.QueryTranslator.GroupingContext;
import org.elasticsearch.xpack.sql.planner.QueryTranslator.QueryTranslation;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupingAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.sql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptSort;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.querydsl.container.TotalCountRef;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.rule.Rule;
import org.elasticsearch.xpack.sql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.util.Assert;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
                new FoldLimit()
                );

        Batch local = new Batch("Local queries",
                new PropagateEmptyLocal(),
                new LocalLimit()
                );

        Batch finish = new Batch("Finish query", Limiter.ONCE,
                new PlanOutputToQueryRef()
                );

        return Arrays.asList(rollup, local, finish);
    }

    private static class FoldProject extends FoldingRule<ProjectExec> {

        @Override
        protected PhysicalPlan rule(ProjectExec project) {
            if (project.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) project.child();
                QueryContainer queryC = exec.queryContainer();
                
                Map<Attribute, Attribute> aliases = new LinkedHashMap<>(queryC.aliases());
                Map<Attribute, ProcessorDefinition> processors = new LinkedHashMap<>(queryC.scalarFunctions());
                
                for (NamedExpression pj : project.projections()) {
                    if (pj instanceof Alias) {
                        Attribute aliasAttr = pj.toAttribute();
                        Expression e = ((Alias) pj).child();

                        if (e instanceof NamedExpression) {
                            Attribute attr = ((NamedExpression) e).toAttribute();
                            aliases.put(aliasAttr, attr);
                            // add placeholder for each scalar function
                            if (e instanceof ScalarFunction) {
                                processors.put(attr, ProcessorDefinitions.toProcessorDefinition(e));
                            }
                        }
                        else {
                            throw new PlanningException("Don't know how to translate expression %s", e);
                        }
                    }
                    else {
                        // for named expressions nothing is recorded as these are resolved last
                        // otherwise 'intermediate' projects might pollute the
                        // output

                        if (pj instanceof ScalarFunction) {
                            ScalarFunction f = (ScalarFunction) pj;
                            processors.put(f.toAttribute(), f.asProcessorDefinition());
                        }
                    }
                }

                QueryContainer clone = new QueryContainer(queryC.query(), queryC.aggs(), queryC.columns(), aliases, queryC.pseudoFunctions(), processors, queryC.sort(), queryC.limit());
                return new EsQueryExec(exec.location(), exec.index(), project.output(), clone);
            }
            return project;
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
                Aggs aggs = addPipelineAggs(qContainer, qt, plan);

                qContainer = new QueryContainer(query, aggs, qContainer.columns(), qContainer.aliases(), 
                        qContainer.pseudoFunctions(), 
                        qContainer.scalarFunctions(), 
                        qContainer.sort(), 
                        qContainer.limit());

                return exec.with(qContainer);
            }
            return plan;
        }

        private Aggs addPipelineAggs(QueryContainer qContainer, QueryTranslation qt, FilterExec fexec) {
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
                        throw new FoldingException(fexec, "Cannot find group for agg %s referrenced by agg filter %s(%s)", refId, filter.name(), filter);
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
                throw new PlanningException("Aggregation filtering not supported (yet) without explicit grouping");
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

                QueryContainer queryC = exec.queryContainer();
                if (groupingContext != null) {
                    queryC = queryC.addGroups(groupingContext.groupMap.values());
                }

                Map<Attribute, Attribute> aliases = new LinkedHashMap<>();
                // tracker for compound aggs seen in a group
                Map<CompoundNumericAggregate, String> compoundAggMap = new LinkedHashMap<>();

                // followed by actual aggregates
                for (NamedExpression ne : a.aggregates()) {
                    
                    // unwrap alias - it can be
                    // - an attribute (since we support aliases inside group-by)
                    //   SELECT emp_no ... GROUP BY emp_no
                    //   SELECT YEAR(hire_date) ... GROUP BY YEAR(hire_date)

                    // - an agg function (typically)
                    //   SELECT COUNT(*), AVG(salary) ... GROUP BY salary;

                    // - a scalar function, which can be applied on an attribute or aggregate and can require one or multiple inputs
                    
                    //   SELECT SIN(emp_no) ... GROUP BY emp_no
                    //   SELECT CAST(YEAR(hire_date)) ... GROUP BY YEAR(hire_date)
                    //   SELECT CAST(AVG(salary)) ... GROUP BY salary
                    //   SELECT AVG(salary) + SIN(MIN(salary)) ... GROUP BY salary

                    if (ne instanceof Alias || ne instanceof Function) {
                        Alias as = ne instanceof Alias ? (Alias) ne : null;
                        Expression child = as != null ? as.child() : ne;

                        // record aliases in case they are later referred in the tree
                        if (as != null && as.child() instanceof NamedExpression) {
                            aliases.put(as.toAttribute(), ((NamedExpression) as.child()).toAttribute());
                        }

                        //
                        // look first for scalar functions which might wrap the actual grouped target
                        // (e.g. 
                        // CAST(field) GROUP BY field or 
                        // ABS(YEAR(field)) GROUP BY YEAR(field) or 
                        // ABS(AVG(salary)) ... GROUP BY salary
                        // )
                        if (child instanceof ScalarFunction) {
                            ScalarFunction f = (ScalarFunction) child;
                            ProcessorDefinition proc = f.asProcessorDefinition();

                            final AtomicReference<QueryContainer> qC = new AtomicReference<>(queryC);

                            proc = proc.transformUp(p -> {
                                // bail out if the def is resolved
                                if (p.resolved()) {
                                    return p;
                                }

                                // get the backing expression and check if it belongs to a agg group or whether it's an expression in the first place
                                Expression exp = p.expression();
                                GroupingAgg matchingGroup = null;
                                if (groupingContext != null) {
                                    // is there a group (aggregation) for this expression ? 
                                    matchingGroup = groupingContext.groupFor(exp);
                                }
                                else {
                                    // a scalar function can be used only if has already been mentioned for grouping
                                    // (otherwise it is the opposite of grouping)
                                    if (exp instanceof ScalarFunction) {
                                        throw new FoldingException(exp, "Scalar function %s can be used only if included already in grouping", exp.toString());
                                    }
                                }

                                // found match for expression; if it's an attribute or scalar, end the processing chain with the reference to the backing agg 
                                if (matchingGroup != null) {
                                    if (exp instanceof Attribute || exp instanceof ScalarFunction) {
                                        Processor action = null;
                                        // special handling of dates since aggs return the typed Date object which needs extraction
                                        // instead of handling this in the scroller, the folder handles this as it already got access to the extraction action
                                        if (exp instanceof DateTimeHistogramFunction) {
                                            action = ((UnaryProcessorDefinition) p).action();
                                        }
                                        return new AggPathInput(exp, matchingGroup.propertyPath(), null, action);
                                    }
                                }
                                // or found an aggregate expression (which has to work on an attribute used for grouping)
                                // (can happen when dealing with a root group)
                                if (Functions.isAggregateFunction(exp)) {
                                    Tuple<QueryContainer, AggPathInput> withFunction = addAggFunction(matchingGroup, (AggregateFunction) exp, compoundAggMap, qC.get());
                                    qC.set(withFunction.v1());
                                    return withFunction.v2();
                                }
                                // not an aggregate and no matching - go to a higher node (likely a function YEAR(birth_date))
                                return p;
                            });
                            
                            if (!proc.resolved()) {
                                throw new FoldingException(child, "Cannot find grouping for '%s'", Expressions.name(child));
                            }

                            // add the computed column
                            queryC = qC.get().addColumn(new ComputedRef(proc));
                            
                            // TODO: is this needed?
                            // redirect the alias to the scalar group id (changing the id altogether doesn't work it is already used in the aggpath)
                            //aliases.put(as.toAttribute(), sf.toAttribute());
                        }
                        // apply the same logic above (for function inputs) to non-scalar functions with small variantions:
                        //  instead of adding things as input, add them as full blown column
                        else {
                            GroupingAgg matchingGroup = null;
                            if (groupingContext != null) {
                                // is there a group (aggregation) for this expression ? 
                                matchingGroup = groupingContext.groupFor(child);
                            }
                            // attributes can only refer to declared groups
                            if (child instanceof Attribute) {
                                Assert.notNull(matchingGroup, "Cannot find group '%s'", Expressions.name(child));
                                queryC = queryC.addAggColumn(matchingGroup.propertyPath());
                            }
                            else {
                                // the only thing left is agg function
                                Assert.isTrue(Functions.isAggregateFunction(child), "Expected aggregate function inside alias; got %s", child.nodeString());
                                Tuple<QueryContainer, AggPathInput> withAgg = addAggFunction(matchingGroup, (AggregateFunction) child, compoundAggMap, queryC);
                                //FIXME: what about inner key
                                queryC = withAgg.v1().addAggColumn(withAgg.v2().context());
                                if (withAgg.v2().innerKey() != null) {
                                    throw new PlanningException("innerkey/matrix stats not handled (yet)");
                                }
                            }
                        }
                    }
                    // not an Alias or a Function, means it's an Attribute so apply the same logic as above
                    else {
                        GroupingAgg matchingGroup = null;
                        if (groupingContext != null) {
                            matchingGroup = groupingContext.groupFor(ne);
                            Assert.notNull(matchingGroup, "Cannot find group '%s'", Expressions.name(ne));
                            queryC = queryC.addAggColumn(matchingGroup.propertyPath());
                        }
                    }
                }

                if (!aliases.isEmpty()) {
                    Map<Attribute, Attribute> newAliases = new LinkedHashMap<>(queryC.aliases());
                    newAliases.putAll(aliases);
                    queryC = queryC.withAliases(newAliases);
                }
                return new EsQueryExec(exec.location(), exec.index(), a.output(), queryC);
            }
            return a;
        }

        private Tuple<QueryContainer, AggPathInput> addAggFunction(GroupingAgg parentAgg, AggregateFunction f, Map<CompoundNumericAggregate, String> compoundAggMap, QueryContainer queryC) {
            String functionId = f.functionId();
            // handle count as a special case agg
            if (f instanceof Count) {
                Count c = (Count) f;
                if (!c.distinct()) {
                    String path = parentAgg == null ? TotalCountRef.PATH : AggPath.bucketCount(parentAgg.asParentPath());
                    Map<String, GroupingAgg> pseudoFunctions = new LinkedHashMap<>(queryC.pseudoFunctions());
                    pseudoFunctions.put(functionId, parentAgg);
                    return new Tuple<>(queryC.withPseudoFunctions(pseudoFunctions), new AggPathInput(f, path));
                }
            }

            AggPathInput aggInput = null;

            // otherwise translate the function into an actual agg
            String parentPath = parentAgg != null ? parentAgg.asParentPath() : null;
            String groupId = parentAgg != null ? parentAgg.id() : null;

            if (f instanceof InnerAggregate) {
                InnerAggregate ia = (InnerAggregate) f;
                CompoundNumericAggregate outer = ia.outer();
                String cAggPath = compoundAggMap.get(outer);

                // the compound agg hasn't been seen before so initialize it
                if (cAggPath == null) {
                    LeafAgg leafAgg = toAgg(parentPath, functionId, outer);
                    cAggPath = leafAgg.propertyPath();
                    compoundAggMap.put(outer, cAggPath);
                    // add the agg (without any reference)
                    queryC = queryC.with(queryC.aggs().addAgg(leafAgg));
                }

                String aggPath = AggPath.metricValue(cAggPath, ia.innerId());
                // FIXME: concern leak - hack around MatrixAgg which is not
                // generalized (afaik)
                aggInput = new AggPathInput(f, aggPath, ia.innerKey() != null ? QueryTranslator.nameOf(ia.innerKey()) : null);
            }
            else {
                LeafAgg leafAgg = toAgg(parentPath, functionId, f);
                aggInput = new AggPathInput(f, leafAgg.propertyPath());
                queryC = queryC.with(queryC.aggs().addAgg(groupId, leafAgg));
            }
            
            return new Tuple<>(queryC, aggInput);
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
                                    throw new PlanningException("does not know how to order by expression %s", ob);
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
                int limit = Foldables.intValueOf(plan.limit());
                int currentSize = exec.queryContainer().limit();
                int newSize = currentSize < 0 ? limit : Math.min(currentSize, limit); 
                return exec.with(exec.queryContainer().withLimit(newSize));
            }
            return plan;
        }
    }

    private static class PlanOutputToQueryRef extends FoldingRule<EsQueryExec> {
        @Override
        protected PhysicalPlan rule(EsQueryExec exec) {
            QueryContainer qContainer = exec.queryContainer();

            // references (aka aggs) are in place
            if (qContainer.hasColumns()) {
                return exec;
            }
                
            for (Attribute attr : exec.output()) {
                qContainer = qContainer.addColumn(attr);
            }

            // after all attributes have been resolved
            return exec.with(qContainer);
        }
    }

    //
    // local
    //
    
    private static class PropagateEmptyLocal extends FoldingRule<PhysicalPlan> {

        @Override
        protected PhysicalPlan rule(PhysicalPlan plan) {
            if (plan.children().size() == 1) {
                PhysicalPlan p = plan.children().get(0);
                if (p instanceof LocalExec && ((LocalExec) p).isEmpty()) {
                    return new LocalExec(plan.location(), new EmptyExecutable(plan.output()));
                }
            }
            return plan;
        }
    }
    
    // local exec currently means empty or one entry so limit can't really be applied
    private static class LocalLimit extends FoldingRule<LimitExec> {

        @Override
        protected PhysicalPlan rule(LimitExec plan) {
            if (plan.child() instanceof LocalExec) {
                return plan.child();
            }
            return plan;
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