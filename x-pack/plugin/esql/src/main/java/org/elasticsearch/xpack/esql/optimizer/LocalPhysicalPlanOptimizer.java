/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.core.util.Queries.Clause;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialDisjoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialIntersects;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.SpatialRelatesUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules.OptimizerRule;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.Stat;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.EsqlTranslatorHandler;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.esql.core.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.optimizer.rules.OptimizerRules.TransformDirection.UP;
import static org.elasticsearch.xpack.esql.plan.physical.EsStatsQueryExec.StatsType.COUNT;

/**
 * Manages field extraction and pushing parts of the query into Lucene. (Query elements that are not pushed into Lucene are executed via
 * the compute engine)
 */
public class LocalPhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, LocalPhysicalOptimizerContext> {
    public static final EsqlTranslatorHandler TRANSLATOR_HANDLER = new EsqlTranslatorHandler();

    private final PhysicalVerifier verifier = PhysicalVerifier.INSTANCE;

    public LocalPhysicalPlanOptimizer(LocalPhysicalOptimizerContext context) {
        super(context);
    }

    public PhysicalPlan localOptimize(PhysicalPlan plan) {
        return verify(execute(plan));
    }

    PhysicalPlan verify(PhysicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    protected List<Batch<PhysicalPlan>> rules(boolean optimizeForEsSource) {
        List<Rule<?, PhysicalPlan>> esSourceRules = new ArrayList<>(4);
        esSourceRules.add(new ReplaceSourceAttributes());

        if (optimizeForEsSource) {
            esSourceRules.add(new PushTopNToSource());
            esSourceRules.add(new PushLimitToSource());
            esSourceRules.add(new PushFiltersToSource());
            esSourceRules.add(new PushStatsToSource());
            esSourceRules.add(new EnableSpatialDistancePushdown());
        }

        // execute the rules multiple times to improve the chances of things being pushed down
        @SuppressWarnings("unchecked")
        var pushdown = new Batch<PhysicalPlan>("Push to ES", esSourceRules.toArray(Rule[]::new));
        // add the field extraction in just one pass
        // add it at the end after all the other rules have ran
        var fieldExtraction = new Batch<>("Field extraction", Limiter.ONCE, new InsertFieldExtraction(), new SpatialDocValuesExtraction());
        return asList(pushdown, fieldExtraction);
    }

    @Override
    protected List<Batch<PhysicalPlan>> batches() {
        return rules(true);
    }

    private static class ReplaceSourceAttributes extends OptimizerRule<EsSourceExec> {

        ReplaceSourceAttributes() {
            super(UP);
        }

        @Override
        protected PhysicalPlan rule(EsSourceExec plan) {
            var docId = new FieldAttribute(plan.source(), EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
            if (plan.indexMode() == IndexMode.TIME_SERIES) {
                Attribute tsid = null, timestamp = null;
                for (Attribute attr : plan.output()) {
                    String name = attr.name();
                    if (name.equals(MetadataAttribute.TSID_FIELD)) {
                        tsid = attr;
                    } else if (name.equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                        timestamp = attr;
                    }
                }
                if (tsid == null || timestamp == null) {
                    throw new IllegalStateException("_tsid or @timestamp are missing from the time-series source");
                }
                return new EsQueryExec(plan.source(), plan.index(), plan.indexMode(), List.of(docId, tsid, timestamp), plan.query());
            } else {
                return new EsQueryExec(plan.source(), plan.index(), plan.indexMode(), List.of(docId), plan.query());
            }
        }
    }

    // Materialize the concrete fields that need to be extracted from the storage until the last possible moment.
    // Expects the local plan to already have a projection containing the fields needed upstream.
    //
    // 1. add the materialization right before usage inside the local plan
    // 2. materialize any missing fields needed further up the chain
    /**
     * @see org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer.ProjectAwayColumns
     */
    static class InsertFieldExtraction extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            // apply the plan locally, adding a field extractor right before data is loaded
            // by going bottom-up
            plan = plan.transformUp(UnaryExec.class, p -> {
                var missing = missingAttributes(p);

                /*
                 * If there is a single grouping then we'll try to use ords. Either way
                 * it loads the field lazily. If we have more than one field we need to
                 * make sure the fields are loaded for the standard hash aggregator.
                 */
                if (p instanceof AggregateExec agg && agg.groupings().size() == 1) {
                    var leaves = new LinkedList<>();
                    // TODO: this seems out of place
                    agg.aggregates()
                        .stream()
                        .filter(a -> agg.groupings().contains(a) == false)
                        .forEach(a -> leaves.addAll(a.collectLeaves()));
                    var remove = agg.groupings().stream().filter(g -> leaves.contains(g) == false).toList();
                    missing.removeAll(Expressions.references(remove));
                }

                // add extractor
                if (missing.isEmpty() == false) {
                    // collect source attributes and add the extractor
                    var extractor = new FieldExtractExec(p.source(), p.child(), List.copyOf(missing));
                    p = p.replaceChild(extractor);
                }

                return p;
            });

            return plan;
        }

        private static Set<Attribute> missingAttributes(PhysicalPlan p) {
            var missing = new LinkedHashSet<Attribute>();
            var input = p.inputSet();

            // collect field attributes used inside expressions
            p.forEachExpression(TypedAttribute.class, f -> {
                if (f instanceof FieldAttribute || f instanceof MetadataAttribute) {
                    if (input.contains(f) == false) {
                        missing.add(f);
                    }
                }
            });
            return missing;
        }
    }

    public static class PushFiltersToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
        FilterExec,
        LocalPhysicalOptimizerContext> {

        @Override
        protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
            PhysicalPlan plan = filterExec;
            if (filterExec.child() instanceof EsQueryExec queryExec) {
                List<Expression> pushable = new ArrayList<>();
                List<Expression> nonPushable = new ArrayList<>();
                for (Expression exp : splitAnd(filterExec.condition())) {
                    (canPushToSource(exp, x -> hasIdenticalDelegate(x, ctx.searchStats())) ? pushable : nonPushable).add(exp);
                }
                if (pushable.size() > 0) { // update the executable with pushable conditions
                    Query queryDSL = TRANSLATOR_HANDLER.asQuery(Predicates.combineAnd(pushable));
                    QueryBuilder planQuery = queryDSL.asBuilder();
                    var query = Queries.combine(Clause.FILTER, asList(queryExec.query(), planQuery));
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
                        plan = new FilterExec(filterExec.source(), queryExec, Predicates.combineAnd(nonPushable));
                    } else { // prune Filter entirely
                        plan = queryExec;
                    }
                } // else: nothing changes
            }

            return plan;
        }

        public static boolean canPushToSource(Expression exp, Predicate<FieldAttribute> hasIdenticalDelegate) {
            if (exp instanceof BinaryComparison bc) {
                return isAttributePushable(bc.left(), bc, hasIdenticalDelegate) && bc.right().foldable();
            } else if (exp instanceof InsensitiveBinaryComparison bc) {
                return isAttributePushable(bc.left(), bc, hasIdenticalDelegate) && bc.right().foldable();
            } else if (exp instanceof BinaryLogic bl) {
                return canPushToSource(bl.left(), hasIdenticalDelegate) && canPushToSource(bl.right(), hasIdenticalDelegate);
            } else if (exp instanceof In in) {
                return isAttributePushable(in.value(), null, hasIdenticalDelegate) && Expressions.foldable(in.list());
            } else if (exp instanceof Not not) {
                return canPushToSource(not.field(), hasIdenticalDelegate);
            } else if (exp instanceof UnaryScalarFunction usf) {
                if (usf instanceof RegexMatch<?> || usf instanceof IsNull || usf instanceof IsNotNull) {
                    if (usf instanceof IsNull || usf instanceof IsNotNull) {
                        if (usf.field() instanceof FieldAttribute fa && fa.dataType().equals(DataType.TEXT)) {
                            return true;
                        }
                    }
                    return isAttributePushable(usf.field(), usf, hasIdenticalDelegate);
                }
            } else if (exp instanceof CIDRMatch cidrMatch) {
                return isAttributePushable(cidrMatch.ipField(), cidrMatch, hasIdenticalDelegate)
                    && Expressions.foldable(cidrMatch.matches());
            } else if (exp instanceof SpatialRelatesFunction bc) {
                return bc.canPushToSource(LocalPhysicalPlanOptimizer::isAggregatable);
            } else if (exp instanceof MatchQueryPredicate mqp) {
                return mqp.field() instanceof FieldAttribute && DataType.isString(mqp.field().dataType());
            }
            return false;
        }

        private static boolean isAttributePushable(
            Expression expression,
            Expression operation,
            Predicate<FieldAttribute> hasIdenticalDelegate
        ) {
            if (isPushableFieldAttribute(expression, hasIdenticalDelegate)) {
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

    /**
     * this method is supposed to be used to define if a field can be used for exact push down (eg. sort or filter).
     * "aggregatable" is the most accurate information we can have from field_caps as of now.
     * Pushing down operations on fields that are not aggregatable would result in an error.
     */
    private static boolean isAggregatable(FieldAttribute f) {
        return f.exactAttribute().field().isAggregatable();
    }

    private static class PushLimitToSource extends OptimizerRule<LimitExec> {
        @Override
        protected PhysicalPlan rule(LimitExec limitExec) {
            PhysicalPlan plan = limitExec;
            PhysicalPlan child = limitExec.child();
            if (child instanceof EsQueryExec queryExec) { // add_task_parallelism_above_query: false
                plan = queryExec.withLimit(limitExec.limit());
            } else if (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
                plan = exchangeExec.replaceChild(queryExec.withLimit(limitExec.limit()));
            }
            return plan;
        }
    }

    private static class PushTopNToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
        TopNExec,
        LocalPhysicalOptimizerContext> {
        @Override
        protected PhysicalPlan rule(TopNExec topNExec, LocalPhysicalOptimizerContext ctx) {
            PhysicalPlan plan = topNExec;
            PhysicalPlan child = topNExec.child();
            if (canPushSorts(child) && canPushDownOrders(topNExec.order(), x -> hasIdenticalDelegate(x, ctx.searchStats()))) {
                var sorts = buildFieldSorts(topNExec.order());
                var limit = topNExec.limit();

                if (child instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
                    plan = exchangeExec.replaceChild(queryExec.withSorts(sorts).withLimit(limit));
                } else {
                    plan = ((EsQueryExec) child).withSorts(sorts).withLimit(limit);
                }
            }
            return plan;
        }

        private boolean canPushDownOrders(List<Order> orders, Predicate<FieldAttribute> hasIdenticalDelegate) {
            // allow only exact FieldAttributes (no expressions) for sorting
            return orders.stream().allMatch(o -> isPushableFieldAttribute(o.child(), hasIdenticalDelegate));
        }

        private List<EsQueryExec.FieldSort> buildFieldSorts(List<Order> orders) {
            List<EsQueryExec.FieldSort> sorts = new ArrayList<>(orders.size());
            for (Order o : orders) {
                sorts.add(new EsQueryExec.FieldSort(((FieldAttribute) o.child()).exactAttribute(), o.direction(), o.nullsPosition()));
            }
            return sorts;
        }
    }

    private static boolean canPushSorts(PhysicalPlan plan) {
        if (plan instanceof EsQueryExec queryExec) {
            return queryExec.canPushSorts();
        }
        if (plan instanceof ExchangeExec exchangeExec && exchangeExec.child() instanceof EsQueryExec queryExec) {
            return queryExec.canPushSorts();
        }
        return false;
    }

    /**
     * Looks for the case where certain stats exist right before the query and thus can be pushed down.
     */
    private static class PushStatsToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
        AggregateExec,
        LocalPhysicalOptimizerContext> {

        @Override
        protected PhysicalPlan rule(AggregateExec aggregateExec, LocalPhysicalOptimizerContext context) {
            PhysicalPlan plan = aggregateExec;
            if (aggregateExec.child() instanceof EsQueryExec queryExec) {
                var tuple = pushableStats(aggregateExec, context);

                // for the moment support pushing count just for one field
                List<Stat> stats = tuple.v2();
                if (stats.size() > 1) {
                    return aggregateExec;
                }

                // TODO: handle case where some aggs cannot be pushed down by breaking the aggs into two sources (regular + stats) + union
                // use the stats since the attributes are larger in size (due to seen)
                if (tuple.v2().size() == aggregateExec.aggregates().size()) {
                    plan = new EsStatsQueryExec(
                        aggregateExec.source(),
                        queryExec.index(),
                        queryExec.query(),
                        queryExec.limit(),
                        tuple.v1(),
                        tuple.v2()
                    );
                }
            }
            return plan;
        }

        private Tuple<List<Attribute>, List<Stat>> pushableStats(AggregateExec aggregate, LocalPhysicalOptimizerContext context) {
            AttributeMap<Stat> stats = new AttributeMap<>();
            Tuple<List<Attribute>, List<Stat>> tuple = new Tuple<>(new ArrayList<>(), new ArrayList<>());

            if (aggregate.groupings().isEmpty()) {
                for (NamedExpression agg : aggregate.aggregates()) {
                    var attribute = agg.toAttribute();
                    Stat stat = stats.computeIfAbsent(attribute, a -> {
                        if (agg instanceof Alias as) {
                            Expression child = as.child();
                            if (child instanceof Count count) {
                                var target = count.field();
                                String fieldName = null;
                                QueryBuilder query = null;
                                // TODO: add count over field (has to be field attribute)
                                if (target.foldable()) {
                                    fieldName = StringUtils.WILDCARD;
                                }
                                // check if regular field
                                else {
                                    if (target instanceof FieldAttribute fa) {
                                        var fName = fa.name();
                                        if (context.searchStats().isSingleValue(fName)) {
                                            fieldName = fa.name();
                                            query = QueryBuilders.existsQuery(fieldName);
                                        }
                                    }
                                }
                                if (fieldName != null) {
                                    return new Stat(fieldName, COUNT, query);
                                }
                            }
                        }
                        return null;
                    });
                    if (stat != null) {
                        List<Attribute> intermediateAttributes = AbstractPhysicalOperationProviders.intermediateAttributes(
                            singletonList(agg),
                            emptyList()
                        );
                        tuple.v1().addAll(intermediateAttributes);
                        tuple.v2().add(stat);
                    }
                }
            }

            return tuple;
        }
    }

    public static boolean hasIdenticalDelegate(FieldAttribute attr, SearchStats stats) {
        return stats.hasIdenticalDelegate(attr.name());
    }

    public static boolean isPushableFieldAttribute(Expression exp, Predicate<FieldAttribute> hasIdenticalDelegate) {
        if (exp instanceof FieldAttribute fa && fa.getExactInfo().hasExact() && isAggregatable(fa)) {
            return fa.dataType() != DataType.TEXT || hasIdenticalDelegate.test(fa);
        }
        return false;
    }

    private static class SpatialDocValuesExtraction extends OptimizerRule<AggregateExec> {
        @Override
        protected PhysicalPlan rule(AggregateExec aggregate) {
            var foundAttributes = new HashSet<FieldAttribute>();

            PhysicalPlan plan = aggregate.transformDown(UnaryExec.class, exec -> {
                if (exec instanceof AggregateExec agg) {
                    var orderedAggregates = new ArrayList<NamedExpression>();
                    var changedAggregates = false;
                    for (NamedExpression aggExpr : agg.aggregates()) {
                        if (aggExpr instanceof Alias as && as.child() instanceof SpatialAggregateFunction af) {
                            if (af.field() instanceof FieldAttribute fieldAttribute
                                && allowedForDocValues(fieldAttribute, agg, foundAttributes)) {
                                // We need to both mark the field to load differently, and change the spatial function to know to use it
                                foundAttributes.add(fieldAttribute);
                                changedAggregates = true;
                                orderedAggregates.add(as.replaceChild(af.withDocValues()));
                            } else {
                                orderedAggregates.add(aggExpr);
                            }
                        } else {
                            orderedAggregates.add(aggExpr);
                        }
                    }
                    if (changedAggregates) {
                        exec = new AggregateExec(
                            agg.source(),
                            agg.child(),
                            agg.groupings(),
                            orderedAggregates,
                            agg.getMode(),
                            agg.estimatedRowSize()
                        );
                    }
                }
                if (exec instanceof EvalExec evalExec) {
                    List<Alias> fields = evalExec.fields();
                    List<Alias> changed = fields.stream()
                        .map(
                            f -> (Alias) f.transformDown(
                                SpatialRelatesFunction.class,
                                spatialRelatesFunction -> (spatialRelatesFunction.hasFieldAttribute(foundAttributes))
                                    ? spatialRelatesFunction.withDocValues(foundAttributes)
                                    : spatialRelatesFunction
                            )
                        )
                        .toList();
                    if (changed.equals(fields) == false) {
                        exec = new EvalExec(exec.source(), exec.child(), changed);
                    }
                }
                if (exec instanceof FilterExec filterExec) {
                    // Note that ST_CENTROID does not support shapes, but SpatialRelatesFunction does, so when we extend the centroid
                    // to support shapes, we need to consider loading shape doc-values for both centroid and relates (ST_INTERSECTS)
                    var condition = filterExec.condition()
                        .transformDown(
                            SpatialRelatesFunction.class,
                            spatialRelatesFunction -> (spatialRelatesFunction.hasFieldAttribute(foundAttributes))
                                ? spatialRelatesFunction.withDocValues(foundAttributes)
                                : spatialRelatesFunction
                        );
                    if (filterExec.condition().equals(condition) == false) {
                        exec = new FilterExec(filterExec.source(), filterExec.child(), condition);
                    }
                }
                if (exec instanceof FieldExtractExec fieldExtractExec) {
                    // Tell the field extractor that it should extract the field from doc-values instead of source values
                    var attributesToExtract = fieldExtractExec.attributesToExtract();
                    Set<Attribute> docValuesAttributes = new HashSet<>();
                    for (Attribute found : foundAttributes) {
                        if (attributesToExtract.contains(found)) {
                            docValuesAttributes.add(found);
                        }
                    }
                    if (docValuesAttributes.size() > 0) {
                        exec = new FieldExtractExec(exec.source(), exec.child(), attributesToExtract, docValuesAttributes);
                    }
                }
                return exec;
            });
            return plan;
        }

        /**
         * This function disallows the use of more than one field for doc-values extraction in the same spatial relation function.
         * This is because comparing two doc-values fields is not supported in the current implementation.
         */
        private boolean allowedForDocValues(FieldAttribute fieldAttribute, AggregateExec agg, Set<FieldAttribute> foundAttributes) {
            var candidateDocValuesAttributes = new HashSet<>(foundAttributes);
            candidateDocValuesAttributes.add(fieldAttribute);
            var spatialRelatesAttributes = new HashSet<FieldAttribute>();
            agg.forEachExpressionDown(SpatialRelatesFunction.class, relatesFunction -> {
                candidateDocValuesAttributes.forEach(candidate -> {
                    if (relatesFunction.hasFieldAttribute(Set.of(candidate))) {
                        spatialRelatesAttributes.add(candidate);
                    }
                });
            });
            // Disallow more than one spatial field to be extracted using doc-values (for now)
            return spatialRelatesAttributes.size() < 2;
        }
    }

    /**
     * When a spatial distance predicate can be pushed down to lucene, this is done by capturing the distance within the same function.
     * In principle this is like re-writing the predicate:
     * <pre>WHERE ST_DISTANCE(field, TO_GEOPOINT("POINT(0 0)")) &lt;= 10000</pre>
     * as:
     * <pre>WHERE ST_INTERSECTS(field, TO_GEOSHAPE("CIRCLE(0,0,10000)"))</pre>
     */
    public static class EnableSpatialDistancePushdown extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
        FilterExec,
        LocalPhysicalOptimizerContext> {

        @Override
        protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
            PhysicalPlan plan = filterExec;
            if (filterExec.child() instanceof EsQueryExec) {
                // Find and rewrite any binary comparisons that involve a distance function and a literal
                var rewritten = filterExec.condition().transformDown(EsqlBinaryComparison.class, comparison -> {
                    ComparisonType comparisonType = ComparisonType.from(comparison.getFunctionType());
                    if (comparison.left() instanceof StDistance dist && comparison.right().foldable()) {
                        return rewriteComparison(comparison, dist, comparison.right(), comparisonType);
                    } else if (comparison.right() instanceof StDistance dist && comparison.left().foldable()) {
                        return rewriteComparison(comparison, dist, comparison.left(), ComparisonType.invert(comparisonType));
                    }
                    return comparison;
                });
                if (rewritten.equals(filterExec.condition()) == false) {
                    plan = new FilterExec(filterExec.source(), filterExec.child(), rewritten);
                }
            }

            return plan;
        }

        private Expression rewriteComparison(
            EsqlBinaryComparison comparison,
            StDistance dist,
            Expression literal,
            ComparisonType comparisonType
        ) {
            Object value = literal.fold();
            if (value instanceof Number number) {
                if (dist.right().foldable()) {
                    return rewriteDistanceFilter(comparison, dist.left(), dist.right(), number, comparisonType);
                } else if (dist.left().foldable()) {
                    return rewriteDistanceFilter(comparison, dist.right(), dist.left(), number, comparisonType);
                }
            }
            return comparison;
        }

        private Expression rewriteDistanceFilter(
            EsqlBinaryComparison comparison,
            Expression spatialExp,
            Expression literalExp,
            Number number,
            ComparisonType comparisonType
        ) {
            Geometry geometry = SpatialRelatesUtils.makeGeometryFromLiteral(literalExp);
            if (geometry instanceof Point point) {
                double distance = number.doubleValue();
                Source source = comparison.source();
                if (comparisonType.lt) {
                    distance = comparisonType.eq ? distance : Math.nextDown(distance);
                    return new SpatialIntersects(source, spatialExp, makeCircleLiteral(point, distance, literalExp));
                } else if (comparisonType.gt) {
                    distance = comparisonType.eq ? distance : Math.nextUp(distance);
                    return new SpatialDisjoint(source, spatialExp, makeCircleLiteral(point, distance, literalExp));
                } else if (comparisonType.eq) {
                    return new And(
                        source,
                        new SpatialIntersects(source, spatialExp, makeCircleLiteral(point, distance, literalExp)),
                        new SpatialDisjoint(source, spatialExp, makeCircleLiteral(point, Math.nextDown(distance), literalExp))
                    );
                }
            }
            return comparison;
        }

        private Literal makeCircleLiteral(Point point, double distance, Expression literalExpression) {
            var circle = new Circle(point.getX(), point.getY(), distance);
            var wkb = WellKnownBinary.toWKB(circle, ByteOrder.LITTLE_ENDIAN);
            return new Literal(literalExpression.source(), new BytesRef(wkb), DataType.GEO_SHAPE);
        }

        /**
         * This enum captures the key differences between various inequalities as perceived from the spatial distance function.
         * In particular, we need to know which direction the inequality points, with lt=true meaning the left is expected to be smaller
         * than the right. And eq=true meaning we expect euality as well. We currently don't support Equals and NotEquals, so the third
         * field disables those.
         */
        enum ComparisonType {
            LTE(true, false, true),
            LT(true, false, false),
            GTE(false, true, true),
            GT(false, true, false),
            EQ(false, false, true);

            private final boolean lt;
            private final boolean gt;
            private final boolean eq;

            ComparisonType(boolean lt, boolean gt, boolean eq) {
                this.lt = lt;
                this.gt = gt;
                this.eq = eq;
            }

            static ComparisonType from(EsqlBinaryComparison.BinaryComparisonOperation op) {
                return switch (op) {
                    case LT -> LT;
                    case LTE -> LTE;
                    case GT -> GT;
                    case GTE -> GTE;
                    default -> EQ;
                };
            }

            static ComparisonType invert(ComparisonType comparisonType) {
                return switch (comparisonType) {
                    case LT -> GT;
                    case LTE -> GTE;
                    case GT -> LT;
                    case GTE -> LTE;
                    default -> EQ;
                };
            }
        }
    }
}
