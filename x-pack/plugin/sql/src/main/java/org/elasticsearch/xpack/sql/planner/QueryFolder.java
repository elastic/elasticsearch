/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.ql.execution.search.AggRef;
import org.elasticsearch.xpack.ql.execution.search.FieldExtraction;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.Functions;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.ql.expression.function.aggregate.InnerAggregate;
import org.elasticsearch.xpack.ql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.AggPathInput;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.ql.expression.gen.pipeline.UnaryPipe;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.expression.literal.IntervalYearMonth;
import org.elasticsearch.xpack.ql.expression.literal.Intervals;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.function.Score;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundNumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.TopHits;
import org.elasticsearch.xpack.sql.expression.function.grouping.Histogram;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeHistogramFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.Year;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.sql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.sql.plan.physical.FilterExec;
import org.elasticsearch.xpack.sql.plan.physical.LimitExec;
import org.elasticsearch.xpack.sql.plan.physical.LocalExec;
import org.elasticsearch.xpack.sql.plan.physical.OrderExec;
import org.elasticsearch.xpack.sql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.sql.plan.physical.PivotExec;
import org.elasticsearch.xpack.sql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.sql.planner.QueryTranslator.QueryTranslation;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByDateHistogram;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByKey;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByNumericHistogram;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByValue;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.container.AggregateSort;
import org.elasticsearch.xpack.sql.querydsl.container.AttributeSort;
import org.elasticsearch.xpack.sql.querydsl.container.ComputedRef;
import org.elasticsearch.xpack.sql.querydsl.container.GlobalCountRef;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef;
import org.elasticsearch.xpack.sql.querydsl.container.GroupByRef.Property;
import org.elasticsearch.xpack.sql.querydsl.container.MetricAggRef;
import org.elasticsearch.xpack.sql.querydsl.container.PivotColumnRef;
import org.elasticsearch.xpack.sql.querydsl.container.QueryContainer;
import org.elasticsearch.xpack.sql.querydsl.container.ScoreSort;
import org.elasticsearch.xpack.sql.querydsl.container.ScriptSort;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Direction;
import org.elasticsearch.xpack.sql.querydsl.container.Sort.Missing;
import org.elasticsearch.xpack.sql.querydsl.container.TopHitsAggRef;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.session.EmptyExecutable;
import org.elasticsearch.xpack.sql.util.Check;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.ql.type.DataType.DATE;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;
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
                new FoldPivot(),
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

                Map<Attribute, Expression> aliases = new LinkedHashMap<>(queryC.aliases());
                Map<Attribute, Pipe> processors = new LinkedHashMap<>(queryC.scalarFunctions());

                for (NamedExpression pj : project.projections()) {
                    if (pj instanceof Alias) {
                        Attribute attr = pj.toAttribute();
                        Expression e = ((Alias) pj).child();

                        // track all aliases (to determine their reference later on)
                        aliases.put(attr, e);

                        // track scalar pipelines
                        if (e instanceof ScalarFunction) {
                            processors.put(attr, ((ScalarFunction) e).asPipe());
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
                        queryC.shouldIncludeFrozen(),
                        queryC.minPageSize());
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
                        qContainer.shouldIncludeFrozen(),
                        qContainer.minPageSize());

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

    // TODO: remove exceptions from the Folder
    static class FoldAggregate extends FoldingRule<AggregateExec> {

        static class GroupingContext {
            final Map<Integer, GroupByKey> groupMap;
            final GroupByKey tail;

            GroupingContext(Map<Integer, GroupByKey> groupMap) {
                this.groupMap = groupMap;

                GroupByKey lastAgg = null;
                for (Entry<Integer, GroupByKey> entry : groupMap.entrySet()) {
                    lastAgg = entry.getValue();
                }

                tail = lastAgg;
            }

            GroupByKey groupFor(Expression exp) {
                Integer hash = null;
                if (Functions.isAggregate(exp)) {
                    AggregateFunction f = (AggregateFunction) exp;
                    // if there's at least one agg in the tree
                    if (groupMap.isEmpty() == false) {
                        GroupByKey matchingGroup = null;
                        // group found - finding the dedicated agg
                        // TODO: when dealing with expressions inside Aggregation, make sure to extract the field
                        hash = Integer.valueOf(f.field().hashCode());
                        matchingGroup = groupMap.get(hash);
                        // return matching group or the tail (last group)
                        return matchingGroup != null ? matchingGroup : tail;
                    } else {
                        return null;
                    }
                }

                hash = Integer.valueOf(exp.hashCode());
                return groupMap.get(hash);
            }

            @Override
            public String toString() {
                return groupMap.toString();
            }
        }

        /**
         * Creates the list of GroupBy keys
         */
        static GroupingContext groupBy(List<? extends Expression> groupings) {
            if (groupings.isEmpty() == true) {
                return null;
            }

            Map<Integer, GroupByKey> aggMap = new LinkedHashMap<>();

            for (Expression exp : groupings) {
                GroupByKey key = null;

                Integer hash = Integer.valueOf(exp.hashCode());
                String aggId = Expressions.id(exp);

                // change analyzed to non non-analyzed attributes
                if (exp instanceof FieldAttribute) {
                    FieldAttribute field = (FieldAttribute) exp;
                    field = field.exactAttribute();
                    key = new GroupByValue(aggId, field.name());
                }

                // handle functions
                else if (exp instanceof Function) {
                    // dates are handled differently because of date histograms
                    if (exp instanceof DateTimeHistogramFunction) {
                        DateTimeHistogramFunction dthf = (DateTimeHistogramFunction) exp;

                        Expression field = dthf.field();
                        if (field instanceof FieldAttribute) {
                            if (dthf.calendarInterval() != null) {
                                key = new GroupByDateHistogram(aggId, QueryTranslator.nameOf(exp), dthf.calendarInterval(), dthf.zoneId());
                            } else {
                                key = new GroupByDateHistogram(aggId, QueryTranslator.nameOf(exp), dthf.fixedInterval(), dthf.zoneId());
                            }
                        }
                        // use scripting for functions
                        else if (field instanceof Function) {
                            ScriptTemplate script = ((Function) field).asScript();
                            if (dthf.calendarInterval() != null) {
                                key = new GroupByDateHistogram(aggId, script, dthf.calendarInterval(), dthf.zoneId());
                            } else {
                                key = new GroupByDateHistogram(aggId, script, dthf.fixedInterval(), dthf.zoneId());
                            }
                        }
                    }
                    // all other scalar functions become a script
                    else if (exp instanceof ScalarFunction) {
                        ScalarFunction sf = (ScalarFunction) exp;
                        key = new GroupByValue(aggId, sf.asScript());
                    }
                    // histogram
                    else if (exp instanceof GroupingFunction) {
                        if (exp instanceof Histogram) {
                            Histogram h = (Histogram) exp;
                            Expression field = h.field();

                            // date histogram
                            if (h.dataType().isDateBased()) {
                                Object value = h.interval().value();
                                // interval of exactly 1 year
                                if (value instanceof IntervalYearMonth
                                        && ((IntervalYearMonth) value).interval().equals(Period.ofYears(1))) {
                                    String calendarInterval = Year.YEAR_INTERVAL;

                                    // When the histogram is `INTERVAL '1' YEAR`, the interval used in the ES date_histogram will be
                                    // a calendar_interval with value "1y". All other intervals will be fixed_intervals expressed in ms.
                                    if (field instanceof FieldAttribute) {
                                        key = new GroupByDateHistogram(aggId, QueryTranslator.nameOf(field), calendarInterval, h.zoneId());
                                    } else if (field instanceof Function) {
                                        key = new GroupByDateHistogram(aggId, ((Function) field).asScript(), calendarInterval, h.zoneId());
                                    }
                                }
                                // typical interval
                                else {
                                    long intervalAsMillis = Intervals.inMillis(h.interval());

                                    // When the histogram in SQL is applied on DATE type instead of DATETIME, the interval
                                    // specified is truncated to the multiple of a day. If the interval specified is less
                                    // than 1 day, then the interval used will be `INTERVAL '1' DAY`.
                                    if (h.dataType() == DATE) {
                                        intervalAsMillis = DateUtils.minDayInterval(intervalAsMillis);
                                    }

                                    if (field instanceof FieldAttribute) {
                                        key = new GroupByDateHistogram(aggId, QueryTranslator.nameOf(field), intervalAsMillis, h.zoneId());
                                    } else if (field instanceof Function) {
                                        key = new GroupByDateHistogram(aggId, ((Function) field).asScript(), intervalAsMillis, h.zoneId());
                                    }
                                }
                            }
                            // numeric histogram
                            else {
                                if (field instanceof FieldAttribute) {
                                    key = new GroupByNumericHistogram(aggId, QueryTranslator.nameOf(field),
                                            Foldables.doubleValueOf(h.interval()));
                                } else if (field instanceof Function) {
                                    key = new GroupByNumericHistogram(aggId, ((Function) field).asScript(),
                                            Foldables.doubleValueOf(h.interval()));
                                }
                            }
                            if (key == null) {
                                throw new SqlIllegalArgumentException("Unsupported histogram field {}", field);
                            }
                        } else {
                            throw new SqlIllegalArgumentException("Unsupproted grouping function {}", exp);
                        }
                    }
                    // bumped into into an invalid function (which should be caught by the verifier)
                    else {
                        throw new SqlIllegalArgumentException("Cannot GROUP BY function {}", exp);
                    }
                }
                // catch corner-case
                else {
                    throw new SqlIllegalArgumentException("Cannot GROUP BY {}", exp);
                }

                aggMap.put(hash, key);
            }
            return new GroupingContext(aggMap);
        }

        @Override
        protected PhysicalPlan rule(AggregateExec a) {
            if (a.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) a.child();
                return fold(a, exec);
            }
            return a;
        }
        
        static EsQueryExec fold(AggregateExec a, EsQueryExec exec) {
            
            QueryContainer queryC = exec.queryContainer();
            
            // track aliases defined in the SELECT and used inside GROUP BY
            // SELECT x AS a ... GROUP BY a
            Map<Attribute, Expression> aliasMap = new LinkedHashMap<>();
            for (NamedExpression ne : a.aggregates()) {
                if (ne instanceof Alias) {
                    aliasMap.put(ne.toAttribute(), ((Alias) ne).child());
                }
            }
            
            if (aliasMap.isEmpty() == false) {
                Map<Attribute, Expression> newAliases = new LinkedHashMap<>(queryC.aliases());
                newAliases.putAll(aliasMap);
                queryC = queryC.withAliases(new AttributeMap<>(newAliases));
            }

            // build the group aggregation
            // NB: any reference in grouping is already "optimized" by its source so there's no need to look for aliases
            GroupingContext groupingContext = groupBy(a.groupings());

            if (groupingContext != null) {
                queryC = queryC.addGroups(groupingContext.groupMap.values());
            }

            // tracker for compound aggs seen in a group
            Map<CompoundNumericAggregate, String> compoundAggMap = new LinkedHashMap<>();

            // followed by actual aggregates
            for (NamedExpression ne : a.aggregates()) {

                // unwrap alias (since we support aliases declared inside SELECTs to be used by the GROUP BY)
                // An alias can point to :
                // - field
                //   SELECT emp_no AS e ... GROUP BY e
                // - a function
                //   SELECT YEAR(hire_date) ... GROUP BY YEAR(hire_date)

                // - an agg function over the grouped field
                //   SELECT COUNT(*), AVG(salary) ... GROUP BY salary;

                // - a scalar function, which can be applied on a column or aggregate and can require one or multiple inputs

                //   SELECT SIN(emp_no) ... GROUP BY emp_no
                //   SELECT CAST(YEAR(hire_date)) ... GROUP BY YEAR(hire_date)
                //   SELECT CAST(AVG(salary)) ... GROUP BY salary
                //   SELECT AVG(salary) + SIN(MIN(salary)) ... GROUP BY salary

                Expression target = ne;

                // unwrap aliases since it's the children we are interested in
                if (ne instanceof Alias) {
                    target = ((Alias) ne).child();
                }

                String id = Expressions.id(target);

                // literal
                if (target.foldable()) {
                    queryC = queryC.addColumn(ne.toAttribute());
                }

                // look at functions
                else if (target instanceof Function) {

                    //
                    // look first for scalar functions which might wrap the actual grouped target
                    // (e.g.
                    // CAST(field) GROUP BY field or
                    // ABS(YEAR(field)) GROUP BY YEAR(field) or
                    // ABS(AVG(salary)) ... GROUP BY salary
                    // )

                    if (target instanceof ScalarFunction) {
                        ScalarFunction f = (ScalarFunction) target;
                        Pipe proc = f.asPipe();

                        final AtomicReference<QueryContainer> qC = new AtomicReference<>(queryC);

                        // traverse the pipe to find the mandatory grouping expression
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
                            } else {
                                // a scalar function can be used only if has already been mentioned for grouping
                                // (otherwise it is the opposite of grouping)
                                // normally this case should be caught by the Verifier
                                if (exp instanceof ScalarFunction) {
                                    throw new FoldingException(exp,
                                            "Scalar function " + exp.toString() + " can be used only if included already in grouping");
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
                                    return new AggPathInput(exp.source(), exp, new GroupByRef(matchingGroup.id(), null, isDateBased),
                                            action);
                                }
                            }
                            // or found an aggregate expression (which has to work on an attribute used for grouping)
                            // (can happen when dealing with a root group)
                            if (Functions.isAggregate(exp)) {
                                Tuple<QueryContainer, AggPathInput> withFunction = addAggFunction(matchingGroup, (AggregateFunction) exp,
                                        compoundAggMap, qC.get());
                                qC.set(withFunction.v1());
                                return withFunction.v2();
                            }
                            // not an aggregate and no matching - go to a higher node (likely a function YEAR(birth_date))
                            return p;
                        });

                        if (proc.resolved() == false) {
                            throw new FoldingException(target, "Cannot find grouping for '{}'", Expressions.name(target));
                        }

                        // add the computed column
                        queryC = qC.get().addColumn(new ComputedRef(proc), id);
                    }

                    // apply the same logic above (for function inputs) to non-scalar functions with small variations:
                    //  instead of adding things as input, add them as full blown column
                    else {
                        GroupByKey matchingGroup = null;
                        if (groupingContext != null) {
                            // is there a group (aggregation) for this expression ?
                            matchingGroup = groupingContext.groupFor(target);
                        }
                        // attributes can only refer to declared groups
                        if (target instanceof Attribute) {
                            Check.notNull(matchingGroup, "Cannot find group [{}]", Expressions.name(target));
                            queryC = queryC.addColumn(new GroupByRef(matchingGroup.id(), null, target.dataType().isDateBased()), id);
                        }
                        // handle histogram
                        else if (target instanceof GroupingFunction) {
                            queryC = queryC.addColumn(new GroupByRef(matchingGroup.id(), null, target.dataType().isDateBased()), id);
                        }
                        // handle literal
                        else if (target.foldable()) {
                            queryC = queryC.addColumn(ne.toAttribute());
                        }
                        // fallback to regular agg functions
                        else {
                            // the only thing left is agg function
                            Check.isTrue(Functions.isAggregate(target), "Expected aggregate function inside alias; got [{}]",
                                    target.nodeString());
                            AggregateFunction af = (AggregateFunction) target;
                            Tuple<QueryContainer, AggPathInput> withAgg = addAggFunction(matchingGroup, af, compoundAggMap, queryC);
                            // make sure to add the inner id (to handle compound aggs)
                            queryC = withAgg.v1().addColumn(withAgg.v2().context(), id);
                        }
                    }

                }
                // not a Function or literal, means its has to be a field or field expression
                else {
                    GroupByKey matchingGroup = null;
                    if (groupingContext != null) {
                        matchingGroup = groupingContext.groupFor(target);
                        Check.notNull(matchingGroup, "Cannot find group [{}]", Expressions.name(ne));

                        queryC = queryC.addColumn(new GroupByRef(matchingGroup.id(), null, ne.dataType().isDateBased()), id);
                    }
                    // fallback
                    else {
                        throw new SqlIllegalArgumentException("Cannot fold aggregate {}", ne);
                    }
                }
            }

            return new EsQueryExec(exec.source(), exec.index(), a.output(), queryC);
        }

        private static Tuple<QueryContainer, AggPathInput> addAggFunction(GroupByKey groupingAgg, AggregateFunction f,
                Map<CompoundNumericAggregate, String> compoundAggMap, QueryContainer queryC) {

            String functionId = Expressions.id(f);
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
                } else if (c.distinct() == false) {
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
                CompoundNumericAggregate outer = (CompoundNumericAggregate) ia.outer();
                String cAggPath = compoundAggMap.get(outer);

                // the compound agg hasn't been seen before so initialize it
                if (cAggPath == null) {
                    LeafAgg leafAgg = toAgg(Expressions.id(outer), outer);
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
                    Expression orderExpression = order.child();

                    // if it's a reference, get the target expression
                    if (orderExpression instanceof ReferenceAttribute) {
                        orderExpression = qContainer.aliases().get(orderExpression);
                    }
                    String lookup = Expressions.id(orderExpression);
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
                        if (orderExpression instanceof ScalarFunction) {
                            ScalarFunction sf = (ScalarFunction) orderExpression;
                            // is there an expression to order by?
                            if (sf.orderBy() != null) {
                                Expression orderBy = sf.orderBy();
                                if (orderBy instanceof NamedExpression) {
                                    orderBy = qContainer.aliases().getOrDefault(orderBy, orderBy);
                                    qContainer = qContainer
                                            .addSort(new AttributeSort(((NamedExpression) orderBy).toAttribute(), direction, missing));
                                } else if (orderBy.foldable() == false) {
                                    // ignore constant
                                    throw new PlanningException("does not know how to order by expression {}", orderBy);
                                }
                            } else {
                                // nope, use scripted sorting
                                qContainer = qContainer.addSort(new ScriptSort(sf.asScript(), direction, missing));
                            }
                        }
                        // score
                        else if (orderExpression instanceof Score) {
                            qContainer = qContainer.addSort(new ScoreSort(direction, missing));
                        }
                        // field
                        else if (orderExpression instanceof FieldAttribute) {
                            qContainer = qContainer.addSort(new AttributeSort((FieldAttribute) orderExpression, direction, missing));
                        }
                        // agg function
                        else if (orderExpression instanceof AggregateFunction) {
                            qContainer = qContainer.addSort(new AggregateSort((AggregateFunction) orderExpression, direction, missing));
                        } else {
                            // unknown
                            throw new SqlIllegalArgumentException("unsupported sorting expression {}", orderExpression);
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


    private static class FoldPivot extends FoldingRule<PivotExec> {

        @Override
        protected PhysicalPlan rule(PivotExec plan) {
            if (plan.child() instanceof EsQueryExec) {
                EsQueryExec exec = (EsQueryExec) plan.child();
                Pivot p = plan.pivot();
                EsQueryExec fold = FoldAggregate
                        .fold(new AggregateExec(plan.source(), exec,
                                new ArrayList<>(p.groupingSet()), combine(p.groupingSet(), p.aggregates())), exec);

                // replace the aggregate extractors with pivot specific extractors
                // these require a reference to the pivoting column in order to compare the value
                // due to the Pivot structure - the column is the last entry in the grouping set
                QueryContainer query = fold.queryContainer();

                List<Tuple<FieldExtraction, String>> fields = new ArrayList<>(query.fields());
                int startingIndex = fields.size() - p.aggregates().size() - 1;
                // pivot grouping
                Tuple<FieldExtraction, String> groupTuple = fields.remove(startingIndex);
                AttributeMap<Literal> values = p.valuesToLiterals();

                for (int i = startingIndex; i < fields.size(); i++) {
                    Tuple<FieldExtraction, String> tuple = fields.remove(i);
                    for (Map.Entry<Attribute, Literal> entry : values.entrySet()) {
                        fields.add(new Tuple<>(
                                new PivotColumnRef(groupTuple.v1(), tuple.v1(), entry.getValue().value()), Expressions.id(entry.getKey())));
                    }
                    i += values.size();
                }

                return fold.with(new QueryContainer(query.query(), query.aggs(),
                        fields,
                        query.aliases(),
                        query.pseudoFunctions(),
                        query.scalarFunctions(),
                        query.sort(),
                        query.limit(),
                        query.shouldTrackHits(),
                        query.shouldIncludeFrozen(),
                        values.size()));
            }
            return plan;
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
