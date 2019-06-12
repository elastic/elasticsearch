/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.execution.search.AggRef;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.AttributeMap;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.ScoreAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundNumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeHistogramFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.AggPathInput;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.UnaryPipe;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
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
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByKey;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.sql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.sql.querydsl.container.GlobalCountRef;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef.Property;
import org.elasticsearch.xpack.sql.querydsl.container.MetricAggRef;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScoreSort;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptSort;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Missing;
import org.elasticsearch.xpack.sql.querydsl.container.TopHitsAggRef;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.rule.Rule;
import org.elasticsearch.xpack.sql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.util.Check;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.sql.planner.QueryTranslator.and;
import static org.elasticsearch.xpack.sql.planner.QueryTranslator.toAgg;
import static org.elasticsearch.xpack.sql.planner.QueryTranslator.toQuery;

/**
 * Folds the PhysicalPlan into a {@link Query}.
 */
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
                Map<Attribute, Pipe> processors = new LinkedHashMap<>(queryC.scalarFunctions());

                for (NamedExpression pj : project.projections()) {
                    if (pj instanceof Alias) {
                        Attribute aliasAttr = pj.toAttribute();
                        Expression e = ((Alias) pj).child();

                        if (e instanceof NamedExpression) {
                            Attribute attr = ((NamedExpression) e).toAttribute();
                            aliases.put(aliasAttr, attr);
                            // add placeholder for each scalar function
                            if (e instanceof ScalarFunction) {
                                processors.put(attr, Expressions.pipe(e));
                            }
                        } else {
                            processors.put(aliasAttr, Expressions.pipe(e));
                        }
                    }
                    else {
                        // for named expressions nothing is recorded as these are resolved last
                        // otherwise 'intermediate' projects might pollute the
                        // output

                        if (pj instanceof ScalarFunction) {
                            ScalarFunction f = (ScalarFunction) pj;
                            processors.put(f.toAttribute(), Expressions.pipe(f));
                        }
                    }
                }

                QueryContainer clone = new QueryContainer(queryC.query(), queryC.aggs(), queryC.fields(),
                        new AttributeMap<>(aliases),
                        queryC.pseudoFunctions(),
                        new AttributeMap<>(processors),
                        queryC.sort(),
                        queryC.limit(),
                        queryC.shouldTrackHits(),
                        queryC.shouldIncludeFrozen());
                return new EsQueryExec(exec.source(), exec.index(), project.output(), clone);
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

                Query query = null;
                if (qContainer.query() != null || qt.query != null) {
                    query = and(plan.source(), qContainer.query(), qt.query);
                }
                Aggs aggs = addPipelineAggs(qContainer, qt, plan);

                qContainer = new QueryContainer(query, aggs, qContainer.fields(),
                        qContainer.aliases(),
                        qContainer.pseudoFunctions(),
                        qContainer.scalarFunctions(),
                        qContainer.sort(),
                        qContainer.limit(),
                        qContainer.shouldTrackHits(),
                        qContainer.shouldIncludeFrozen());

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
            else {
                aggs = aggs.addAgg(filter);
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
                            Pipe proc = f.asPipe();

                            final AtomicReference<QueryContainer> qC = new AtomicReference<>(queryC);

                            proc = proc.transformUp(p -> {
                                // bail out if the def is resolved
                                if (p.resolved()) {
                                    return p;
                                }

                                // get the backing expression and check if it belongs to a agg group or whether it's
                                // an expression in the first place
                                Expression exp = p.expression();
                                GroupByKey matchingGroup = null;
                                if (groupingContext != null) {
                                    // is there a group (aggregation) for this expression ?
                                    matchingGroup = groupingContext.groupFor(exp);
                                }
                                else {
                                    // a scalar function can be used only if has already been mentioned for grouping
                                    // (otherwise it is the opposite of grouping)
                                    if (exp instanceof ScalarFunction) {
                                        throw new FoldingException(exp, "Scalar function " +exp.toString()
                                                + " can be used only if included already in grouping");
                                    }
                                }

                                // found match for expression; if it's an attribute or scalar, end the processing chain with
                                // the reference to the backing agg
                                if (matchingGroup != null) {
                                    if (exp instanceof Attribute || exp instanceof ScalarFunction || exp instanceof GroupingFunction) {
                                        Processor action = null;
                                        boolean isDateBased = exp.dataType().isDateBased();
                                        /*
                                         * special handling of dates since aggs return the typed Date object which needs
                                         * extraction instead of handling this in the scroller, the folder handles this
                                         * as it already got access to the extraction action
                                         */
                                        if (exp instanceof DateTimeHistogramFunction) {
                                            action = ((UnaryPipe) p).action();
                                            isDateBased = true;
                                        }
                                        return new AggPathInput(exp.source(), exp,
                                            new GroupByRef(matchingGroup.id(), null, isDateBased), action);
                                    }
                                }
                                // or found an aggregate expression (which has to work on an attribute used for grouping)
                                // (can happen when dealing with a root group)
                                if (Functions.isAggregate(exp)) {
                                    Tuple<QueryContainer, AggPathInput> withFunction = addAggFunction(matchingGroup,
                                            (AggregateFunction) exp, compoundAggMap, qC.get());
                                    qC.set(withFunction.v1());
                                    return withFunction.v2();
                                }
                                // not an aggregate and no matching - go to a higher node (likely a function YEAR(birth_date))
                                return p;
                            });

                            if (!proc.resolved()) {
                                throw new FoldingException(child, "Cannot find grouping for '{}'", Expressions.name(child));
                            }

                            // add the computed column
                            queryC = qC.get().addColumn(new ComputedRef(proc), f.toAttribute());

                            // TODO: is this needed?
                            // redirect the alias to the scalar group id (changing the id altogether doesn't work it is
                            // already used in the aggpath)
                            //aliases.put(as.toAttribute(), sf.toAttribute());
                        }
                        // apply the same logic above (for function inputs) to non-scalar functions with small variations:
                        //  instead of adding things as input, add them as full blown column
                        else {
                            GroupByKey matchingGroup = null;
                            if (groupingContext != null) {
                                // is there a group (aggregation) for this expression ?
                                matchingGroup = groupingContext.groupFor(child);
                            }
                            // attributes can only refer to declared groups
                            if (child instanceof Attribute) {
                                Check.notNull(matchingGroup, "Cannot find group [{}]", Expressions.name(child));
                                queryC = queryC.addColumn(
                                    new GroupByRef(matchingGroup.id(), null, child.dataType().isDateBased()), ((Attribute) child));
                            }
                            // handle histogram
                            else if (child instanceof GroupingFunction) {
                                queryC = queryC.addColumn(new GroupByRef(matchingGroup.id(), null, child.dataType().isDateBased()),
                                        ((GroupingFunction) child).toAttribute());
                            }
                            // fallback to regular agg functions
                            else {
                                // the only thing left is agg function
                                Check.isTrue(Functions.isAggregate(child),
                                        "Expected aggregate function inside alias; got [{}]", child.nodeString());
                                AggregateFunction af = (AggregateFunction) child;
                                Tuple<QueryContainer, AggPathInput> withAgg = addAggFunction(matchingGroup, af, compoundAggMap, queryC);
                                // make sure to add the inner id (to handle compound aggs)
                                queryC = withAgg.v1().addColumn(withAgg.v2().context(), af.toAttribute());
                            }
                        }
                    // not an Alias or Function means it's an Attribute so apply the same logic as above
                    } else {
                        GroupByKey matchingGroup = null;
                        if (groupingContext != null) {
                            matchingGroup = groupingContext.groupFor(ne);
                            Check.notNull(matchingGroup, "Cannot find group [{}]", Expressions.name(ne));

                            queryC = queryC.addColumn(
                                new GroupByRef(matchingGroup.id(), null, ne.dataType().isDateBased()), ne.toAttribute());
                        }
                    }
                }

                if (!aliases.isEmpty()) {
                    Map<Attribute, Attribute> newAliases = new LinkedHashMap<>(queryC.aliases());
                    newAliases.putAll(aliases);
                    queryC = queryC.withAliases(new AttributeMap<>(newAliases));
                }
                return new EsQueryExec(exec.source(), exec.index(), a.output(), queryC);
            }
            return a;
        }

        private Tuple<QueryContainer, AggPathInput> addAggFunction(GroupByKey groupingAgg, AggregateFunction f,
                Map<CompoundNumericAggregate, String> compoundAggMap, QueryContainer queryC) {
            String functionId = f.functionId();
            // handle count as a special case agg
            if (f instanceof Count) {
                Count c = (Count) f;
                // COUNT(*) or COUNT(<literal>)
                if (c.field().foldable()) {
                    AggRef ref = null;

                    if (groupingAgg == null) {
                        ref = GlobalCountRef.INSTANCE;
                        // if the count points to the total track hits, enable accurate count retrieval
                        queryC = queryC.withTrackHits();
                    } else {
                        ref = new GroupByRef(groupingAgg.id(), Property.COUNT, false);
                    }

                    Map<String, GroupByKey> pseudoFunctions = new LinkedHashMap<>(queryC.pseudoFunctions());
                    pseudoFunctions.put(functionId, groupingAgg);
                    return new Tuple<>(queryC.withPseudoFunctions(pseudoFunctions), new AggPathInput(f, ref));
                // COUNT(<field_name>)
                } else if (!c.distinct()) {
                    LeafAgg leafAgg = toAgg(functionId, f);
                    AggPathInput a = new AggPathInput(f, new MetricAggRef(leafAgg.id(), "doc_count", "_count", false));
                    queryC = queryC.with(queryC.aggs().addAgg(leafAgg));
                    return new Tuple<>(queryC, a);
                }
                // the only variant left - COUNT(DISTINCT) - will be covered by the else branch below as it maps to an aggregation
            }

            AggPathInput aggInput = null;

            if (f instanceof InnerAggregate) {
                InnerAggregate ia = (InnerAggregate) f;
                CompoundNumericAggregate outer = ia.outer();
                String cAggPath = compoundAggMap.get(outer);

                // the compound agg hasn't been seen before so initialize it
                if (cAggPath == null) {
                    LeafAgg leafAgg = toAgg(outer.functionId(), outer);
                    cAggPath = leafAgg.id();
                    compoundAggMap.put(outer, cAggPath);
                    // add the agg (without any reference)
                    queryC = queryC.with(queryC.aggs().addAgg(leafAgg));
                }

                // FIXME: concern leak - hack around MatrixAgg which is not
                // generalized (afaik)
                aggInput = new AggPathInput(f,
                        new MetricAggRef(cAggPath, ia.innerName(),
                            ia.innerKey() != null ? QueryTranslator.nameOf(ia.innerKey()) : null,
                            ia.dataType().isDateBased()));
            }
            else {
                LeafAgg leafAgg = toAgg(functionId, f);
                if (f instanceof TopHits) {
                    aggInput = new AggPathInput(f, new TopHitsAggRef(leafAgg.id(), f.dataType()));
                } else {
                    aggInput = new AggPathInput(f, new MetricAggRef(leafAgg.id(), f.dataType().isDateBased()));
                }
                queryC = queryC.with(queryC.aggs().addAgg(leafAgg));
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
                    Direction direction = Direction.from(order.direction());
                    Missing missing = Missing.from(order.nullsPosition());

                    // check whether sorting is on an group (and thus nested agg) or field
                    Attribute attr = ((NamedExpression) order.child()).toAttribute();
                    // check whether there's an alias (occurs with scalar functions which are not named)
                    attr = qContainer.aliases().getOrDefault(attr, attr);
                    String lookup = attr.id().toString();
                    GroupByKey group = qContainer.findGroupForAgg(lookup);

                    // TODO: might need to validate whether the target field or group actually exist
                    if (group != null && group != Aggs.IMPLICIT_GROUP_KEY) {
                        // check whether the lookup matches a group
                        if (group.id().equals(lookup)) {
                            qContainer = qContainer.updateGroup(group.with(direction));
                        }
                        // else it's a leafAgg
                        else {
                            qContainer = qContainer.updateGroup(group.with(direction));
                        }
                    }
                    else {
                        // scalar functions typically require script ordering
                        if (attr instanceof ScalarFunctionAttribute) {
                            ScalarFunctionAttribute sfa = (ScalarFunctionAttribute) attr;
                            // is there an expression to order by?
                            if (sfa.orderBy() != null) {
                                if (sfa.orderBy() instanceof NamedExpression) {
                                    Attribute at = ((NamedExpression) sfa.orderBy()).toAttribute();
                                    at = qContainer.aliases().getOrDefault(at, at);
                                    qContainer = qContainer.addSort(new AttributeSort(at, direction, missing));
                                } else if (!sfa.orderBy().foldable()) {
                                    // ignore constant
                                    throw new PlanningException("does not know how to order by expression {}", sfa.orderBy());
                                }
                            } else {
                                // nope, use scripted sorting
                                qContainer = qContainer.addSort(new ScriptSort(sfa.script(), direction, missing));
                            }
                        } else if (attr instanceof ScoreAttribute) {
                            qContainer = qContainer.addSort(new ScoreSort(direction, missing));
                        } else {
                            qContainer = qContainer.addSort(new AttributeSort(attr, direction, missing));
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
                if (p instanceof LocalExec) {
                    if (((LocalExec) p).isEmpty()) {
                        return new LocalExec(plan.source(), new EmptyExecutable(plan.output()));
                    } else {
                        throw new SqlIllegalArgumentException("Encountered a bug; {} is a LocalExec but is not empty", p);
                    }
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
