/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.planner;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.BinaryExpression;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.NestedFieldAttribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.sql.expression.function.aggregate.CompoundNumericAggregate;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.aggregate.ExtendedStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.MatrixStats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileRanks;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Percentiles;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Stats;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunctionAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.Params;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.predicate.And;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryComparison;
import org.elasticsearch.xpack.sql.expression.predicate.Equals;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThan;
import org.elasticsearch.xpack.sql.expression.predicate.GreaterThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.LessThan;
import org.elasticsearch.xpack.sql.expression.predicate.LessThanOrEqual;
import org.elasticsearch.xpack.sql.expression.predicate.Not;
import org.elasticsearch.xpack.sql.expression.predicate.Or;
import org.elasticsearch.xpack.sql.expression.predicate.Range;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.expression.regex.Like;
import org.elasticsearch.xpack.sql.expression.regex.RLike;
import org.elasticsearch.xpack.sql.querydsl.agg.AggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AggPath;
import org.elasticsearch.xpack.sql.querydsl.agg.AndAggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.AvgAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.CardinalityAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.ExtendedStatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByColumnAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupByDateAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.GroupingAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.LeafAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MatrixStatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MaxAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.MinAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.OrAggFilter;
import org.elasticsearch.xpack.sql.querydsl.agg.PercentileRanksAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.PercentilesAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.StatsAgg;
import org.elasticsearch.xpack.sql.querydsl.agg.SumAgg;
import org.elasticsearch.xpack.sql.querydsl.query.AndQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.sql.querydsl.query.MultiMatchQuery;
import org.elasticsearch.xpack.sql.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.sql.querydsl.query.NotQuery;
import org.elasticsearch.xpack.sql.querydsl.query.OrQuery;
import org.elasticsearch.xpack.sql.querydsl.query.Query;
import org.elasticsearch.xpack.sql.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.sql.querydsl.query.RangeQuery;
import org.elasticsearch.xpack.sql.querydsl.query.RegexQuery;
import org.elasticsearch.xpack.sql.querydsl.query.ScriptQuery;
import org.elasticsearch.xpack.sql.querydsl.query.TermQuery;
import org.elasticsearch.xpack.sql.querydsl.query.WildcardQuery;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.elasticsearch.xpack.sql.util.Assert;
import org.elasticsearch.xpack.sql.util.ReflectionUtils;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.sql.expression.Foldables.doubleValuesOf;
import static org.elasticsearch.xpack.sql.expression.Foldables.stringValueOf;
import static org.elasticsearch.xpack.sql.expression.Foldables.valueOf;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate.formatTemplate;

abstract class QueryTranslator {

    static final List<ExppressionTranslator<?>> QUERY_TRANSLATORS = Arrays.asList(
            new BinaryComparisons(),
            new Ranges(),
            new BinaryLogic(),
            new Nots(),
            new Likes(), 
            new StringQueries(),
            new Matches(),
            new MultiMatches()
            );

    static final List<AggTranslator<?>> AGG_TRANSLATORS = Arrays.asList(
            new Maxes(),
            new Mins(),
            new Avgs(),
            new Sums(),
            new StatsAggs(),
            new ExtendedStatsAggs(),
            new MatrixStatsAggs(),
            new PercentilesAggs(), 
            new PercentileRanksAggs(),
            new DistinctCounts(),
            new DateTimes()
            );

    static class QueryTranslation {
        final Query query;
        // Agg filter / Function or Agg association
        final AggFilter aggFilter;

        QueryTranslation(Query query) {
            this(query, null);
        }

        QueryTranslation(AggFilter aggFilter) {
            this(null, aggFilter);
        }

        QueryTranslation(Query query, AggFilter aggFilter) {
            this.query = query;
            this.aggFilter = aggFilter;
        }
    }

    static QueryTranslation toQuery(Expression e, boolean onAggs) {
        QueryTranslation translation = null;
        for (ExppressionTranslator<?> translator : QUERY_TRANSLATORS) {
            translation = translator.translate(e, onAggs);
            if (translation != null) {
                return translation;
            }
        }

        throw new UnsupportedOperationException(format(Locale.ROOT, "Don't know how to translate %s %s", e.nodeName(), e));
    }

    static LeafAgg toAgg(String parent, String id, Function f) {

        for (AggTranslator<?> translator : AGG_TRANSLATORS) {
            LeafAgg agg = translator.apply(id, parent, f);
            if (agg != null) {
                return agg;
            }
        }

        throw new UnsupportedOperationException(format(Locale.ROOT, "Don't know how to translate %s %s", f.nodeName(), f));
    }

    static class GroupingContext {
        final GroupingAgg head;
        final GroupingAgg tail;
        final ExpressionId headAggId;

        final Map<ExpressionId, GroupingAgg> groupMap;
        final List<String> aggNames;
        final String groupPath;

        GroupingContext(Map<ExpressionId, GroupingAgg> groupMap, String propertyPath) {
            this.groupMap = groupMap;
            this.groupPath = propertyPath;

            aggNames = groupMap.values().stream()
                        .map(i -> i.id())
                        .collect(toList());

            Iterator<Entry<ExpressionId, GroupingAgg>> iterator = groupMap.entrySet().iterator();

            Entry<ExpressionId, GroupingAgg> entry = iterator.next();
            headAggId = entry.getKey();
            head = entry.getValue();

            GroupingAgg lastAgg = head;

            while (iterator.hasNext()) {
                lastAgg = iterator.next().getValue();
            }
            tail = lastAgg;
        }


        GroupingAgg parentGroupFor(Expression exp) {
            if (Functions.isAggregateFunction(exp)) {
                AggregateFunction f = (AggregateFunction) exp;
                // if there's at least one agg in the tree
                if (groupPath != null) {
                    GroupingAgg matchingGroup = null;
                    // group found - finding the dedicated agg
                    if (f.field() instanceof NamedExpression) {
                        matchingGroup = groupMap.get(((NamedExpression) f.field()).id());
                    }
                    // return matching group or the tail (last group)
                    return matchingGroup != null ? matchingGroup : tail;
                }
                else {
                    return null;
                }
            }
            if (exp instanceof NamedExpression) {
                return groupMap.get(((NamedExpression) exp).id());
            }
            throw new SqlIllegalArgumentException("Don't know how to find group for expression %s", exp);
        }

        @Override
        public String toString() {
            return groupMap.toString();
        }
    }

    // creates a tree of GroupBy aggs plus some extra information
    // useful for tree validation/group referencing
    static GroupingContext groupBy(List<? extends Expression> groupings) {
        if (groupings.isEmpty()) {
            return null;
        }

        Map<ExpressionId, GroupingAgg> aggMap = new LinkedHashMap<>();

        // nested the aggs but also
        // identify each agg by an expression for later referencing
        String propertyPath = "";
        for (Expression exp : groupings) {
            String aggId;
            if (exp instanceof NamedExpression) {
                NamedExpression ne = (NamedExpression) exp;

                if (exp instanceof FieldAttribute) {
                    FieldAttribute fa = (FieldAttribute) exp;
                    if (fa.isAnalyzed()) {
                        ne = fa.notAnalyzedAttribute();
                    }
                }
                aggId = ne.id().toString();
                
                propertyPath = AggPath.path(propertyPath, aggId);

                GroupingAgg agg = null;
                
                // dates are handled differently because of date histograms
                if (exp instanceof DateTimeFunction) {
                    DateTimeFunction dtf = (DateTimeFunction) exp;
                    agg = new GroupByDateAgg(aggId, AggPath.bucketValue(propertyPath), nameOf(exp), dtf.interval(), dtf.timeZone());
                }
                else {
                    agg = new GroupByColumnAgg(aggId, AggPath.bucketValue(propertyPath), ne.name());
                }
                
                aggMap.put(ne.id(), agg);
            }
            else {
                throw new SqlIllegalArgumentException("Don't know how to group on %s", exp.nodeString());
            }
        }
        return new GroupingContext(aggMap, propertyPath);
    }

    static QueryTranslation and(Location loc, QueryTranslation left, QueryTranslation right) {
        Assert.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }

        Query newQ = null;
        if (left.query != null || right.query != null) {
            newQ = and(loc, left.query, right.query);
        }

        AggFilter aggFilter = null;

        if (left.aggFilter == null) {
            aggFilter = right.aggFilter;
        }
        else if (right.aggFilter == null) {
            aggFilter = left.aggFilter;
        }
        else {
            aggFilter = new AndAggFilter(left.aggFilter, right.aggFilter);
        }

        return new QueryTranslation(newQ, aggFilter);
    }

    static Query and(Location loc, Query left, Query right) {
        Assert.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return new AndQuery(loc, left, right);
    }

    static QueryTranslation or(Location loc, QueryTranslation left, QueryTranslation right) {
        Assert.isTrue(left != null || right != null, "Both expressions are null");
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }

        Query newQ = null;
        if (left.query != null || right.query != null) {
            newQ = or(loc, left.query, right.query);
        }

        AggFilter aggFilter = null;

        if (left.aggFilter == null) {
            aggFilter = right.aggFilter;
        }
        else if (right.aggFilter == null) {
            aggFilter = left.aggFilter;
        }
        else {
            aggFilter = new OrAggFilter(left.aggFilter, right.aggFilter);
        }

        return new QueryTranslation(newQ, aggFilter);
    }

    static Query or(Location loc, Query left, Query right) {
        Assert.isTrue(left != null || right != null, "Both expressions are null");

        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return new OrQuery(loc, left, right);
    }

    static Query not(Query query) {
        Assert.isTrue(query != null, "Expressions is null");
        return new NotQuery(query.location(), query);
    }

    static String nameOf(Expression e) {
        if (e instanceof DateTimeFunction) {
            return nameOf(((DateTimeFunction) e).argument());
        }
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).name();
        }
        if (e instanceof Literal) {
            return String.valueOf(e.fold());
        }
        throw new SqlIllegalArgumentException("Cannot determine name for %s", e);
    }

    static String idOf(Expression e) {
        if (e instanceof NamedExpression) {
            return ((NamedExpression) e).id().toString();
        }
        throw new SqlIllegalArgumentException("Cannot determine id for %s", e);
    }

    @SuppressWarnings("rawtypes")
    static ColumnProcessor matrixFieldExtractor(Expression exp) {
        String key = nameOf(exp);
        return r -> r instanceof Map ? ((Map) r).get(key) : r;
    }

    static String dateFormat(Expression e) {
        if (e instanceof DateTimeFunction) {
            return ((DateTimeFunction) e).dateTimeFormat();
        }
        return null;
    }

    static String field(AggregateFunction af) {
        Expression arg = af.field();
        if (arg instanceof RootFieldAttribute) {
            return ((RootFieldAttribute) arg).name();
        }
        if (arg instanceof Literal) {
            return String.valueOf(((Literal) arg).value());
        }
        throw new SqlIllegalArgumentException("Does not know how to convert argument %s for functon %s", arg.nodeString(), af.nodeString());
    }

    // TODO: need to optimize on ngram
    // TODO: see whether escaping is needed
    static class Likes extends ExppressionTranslator<BinaryExpression> {
    
        @Override
        protected QueryTranslation asQuery(BinaryExpression e, boolean onAggs) {
            Query q = null;
            boolean analyzed = true;
            String target = null;
    
            if (e.left() instanceof FieldAttribute) {
                FieldAttribute fa = (FieldAttribute) e.left();
                analyzed = fa.isAnalyzed();
                target = nameOf(analyzed ? fa : fa.notAnalyzedAttribute());
            }
    
            String pattern = sqlToEsPatternMatching(stringValueOf(e.right()));
            if (e instanceof Like) {
                if (analyzed) {
                    q = new QueryStringQuery(e.location(), pattern, target);
                }
                else {
                    q = new WildcardQuery(e.location(), nameOf(e.left()), pattern);
                }
            }
    
            if (e instanceof RLike) {
                if (analyzed) {
                    q = new QueryStringQuery(e.location(), "/" + pattern + "/", target);
                }
                else {
                    q = new RegexQuery(e.location(), nameOf(e.left()), sqlToEsPatternMatching(stringValueOf(e.right())));
                }
            }
    
            return q != null ? new QueryTranslation(wrapIfNested(q, e.left())) : null;
        }
    
        private static String sqlToEsPatternMatching(String pattern) {
            return pattern.replace("%", "*").replace("_", "?");
        }
    }
    
    static class StringQueries extends ExppressionTranslator<StringQueryPredicate> {
    
        @Override
        protected QueryTranslation asQuery(StringQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(new QueryStringQuery(q.location(), q.query(), q.fields(), q));
        }
    }
    
    static class Matches extends ExppressionTranslator<MatchQueryPredicate> {
    
        @Override
        protected QueryTranslation asQuery(MatchQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(wrapIfNested(new MatchQuery(q.location(), nameOf(q.field()), q.query(), q), q.field()));
        }
    }
    
    static class MultiMatches extends ExppressionTranslator<MultiMatchQueryPredicate> {
    
        @Override
        protected QueryTranslation asQuery(MultiMatchQueryPredicate q, boolean onAggs) {
            return new QueryTranslation(new MultiMatchQuery(q.location(), q.query(), q.fields(), q));
        }
    }
    
    static class BinaryLogic extends ExppressionTranslator<BinaryExpression> {
    
        @Override
        protected QueryTranslation asQuery(BinaryExpression e, boolean onAggs) {
            if (e instanceof And) {
                return and(e.location(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
            }
            if (e instanceof Or) {
                return or(e.location(), toQuery(e.left(), onAggs), toQuery(e.right(), onAggs));
            }
    
            return null;
        }
    }
    
    static class Nots extends ExppressionTranslator<Not> {
    
        @Override
        protected QueryTranslation asQuery(Not not, boolean onAggs) {
            QueryTranslation translation = toQuery(not.child(), onAggs);
            return new QueryTranslation(not(translation.query), translation.aggFilter);
        }
    }
    
    // assume the Optimizer properly orders the predicates to ease the translation
    static class BinaryComparisons extends ExppressionTranslator<BinaryComparison> {
    
        @Override
        protected QueryTranslation asQuery(BinaryComparison bc, boolean onAggs) {
            Assert.isTrue(bc.right() instanceof Literal, "don't know how to translate right %s in %s", bc.right().nodeString(), bc);
    
            if (bc.left() instanceof NamedExpression) {
                NamedExpression ne = (NamedExpression) bc.left();
    
                Query query = null;
                AggFilter aggFilter = null;
    
                Attribute at = ne.toAttribute();
                
                // scalar function can appear in both WHERE and HAVING so handle it first
                // in both cases the function script is used - script-query/query for the former, bucket-selector/aggFilter for the latter
                
                if (at instanceof ScalarFunctionAttribute) {
                    ScalarFunctionAttribute sfa = (ScalarFunctionAttribute) at;
                    ScriptTemplate scriptTemplate = sfa.script();
    
                    String template = formatTemplate("%s %s {}", scriptTemplate.template(), bc.symbol());
                    // no need to bind the wrapped/target agg - it is already available through the nested script (needed to create the script itself)
                    Params params = paramsBuilder().script(scriptTemplate.params()).variable(valueOf(bc.right())).build();
                    ScriptTemplate script = new ScriptTemplate(template, params, DataTypes.BOOLEAN);
                    if (onAggs) {
                        aggFilter = new AggFilter(at.id().toString(), script);
                    }
                    else {
                        query = new ScriptQuery(at.location(), script);
                    }
                }
    
                //
                // Agg context means HAVING -> PipelineAggs
                //
                else if (onAggs) {
                    String template = null;
                    Params params = null;
                    
                    // agg function
                    if (at instanceof AggregateFunctionAttribute) {
                        AggregateFunctionAttribute fa = (AggregateFunctionAttribute) at;
    
                        // TODO: handle case where both sides of the comparison are functions
                        template = formatTemplate("{} %s {}", bc.symbol());
    
                        // bind the agg and the variable to the script
                        params = paramsBuilder().agg(fa.functionId(), fa.propertyPath()).variable(valueOf(bc.right())).build();
                    }
    
                    aggFilter = new AggFilter(at.id().toString(), new ScriptTemplate(template, params, DataTypes.BOOLEAN));
                }
    
                //
                // No Agg context means WHERE clause
                //
                else {
                    if (at instanceof FieldAttribute) {
                        query = wrapIfNested(translateQuery(bc), ne);
                    }
                }
                
                return new QueryTranslation(query, aggFilter);
            }
    
            else {
                throw new UnsupportedOperationException("No idea how to translate " + bc.left());
            }
        }
    
        private static Query translateQuery(BinaryComparison bc) {
            Location loc = bc.location();
            String name = nameOf(bc.left());
            Object value = valueOf(bc.right());
            String format = dateFormat(bc.left());
    
            if (bc instanceof GreaterThan) {
                return new RangeQuery(loc, name, value, false, null, false, format);
            }
            if (bc instanceof GreaterThanOrEqual) {
                return new RangeQuery(loc, name, value, true, null, false, format);
            }
            if (bc instanceof LessThan) {
                return new RangeQuery(loc, name, null, false, value, false, format);
            }
            if (bc instanceof LessThanOrEqual) {
                return new RangeQuery(loc, name, null, false, value, true, format);
            }
            if (bc instanceof Equals) {
                if (bc.left() instanceof FieldAttribute) {
                    FieldAttribute fa = (FieldAttribute) bc.left();
                    if (fa.isAnalyzed()) {
                        return new MatchQuery(loc, name, value);
                    }
                }
                return new TermQuery(loc, name, value);
            }
    
            Assert.isTrue(false, "don't know how to translate binary comparison %s in %s", bc.right().nodeString(), bc);
            return null;
        }
    }
    
    static class Ranges extends ExppressionTranslator<Range> {
    
        @Override
        protected QueryTranslation asQuery(Range r, boolean onAggs) {
            Object lower = valueOf(r.lower());
            Object upper = valueOf(r.upper());
    
            Expression e = r.value();
    
    
            if (e instanceof NamedExpression) {
                NamedExpression ne = (NamedExpression) e;
                
                Query query = null;
                AggFilter aggFilter = null;
    
                Attribute at = ne.toAttribute();
    
                // scalar function can appear in both WHERE and HAVING so handle it first
                // in both cases the function script is used - script-query/query for the former, bucket-selector/aggFilter for the latter
    
                if (at instanceof ScalarFunctionAttribute) {
                    ScalarFunctionAttribute sfa = (ScalarFunctionAttribute) at;
                    ScriptTemplate scriptTemplate = sfa.script();
    
                    String template = formatTemplate("({} %s %s) && (%s %s {})", 
                            r.includeLower() ? "<=" : "<",
                            scriptTemplate.template(),
                            scriptTemplate.template(),
                            r.includeUpper() ? "<=" : "<");
    
                    // no need to bind the wrapped/target - it is already available through the nested script (needed to create the script itself)
                    Params params = paramsBuilder().variable(lower)
                            .script(scriptTemplate.params())
                            .script(scriptTemplate.params())
                            .variable(upper)
                            .build();
                    
                    ScriptTemplate script = new ScriptTemplate(template, params, DataTypes.BOOLEAN);
                    
                    if (onAggs) {
                        aggFilter = new AggFilter(at.id().toString(), script);
                    }
                    else {
                        query = new ScriptQuery(at.location(), script);
                    }
                }
                
                //
                // HAVING
                //
                else if (onAggs) {
                    String template = null;
                    Params params = null;
    
                    // agg function
                    if (at instanceof AggregateFunctionAttribute) {
                        AggregateFunctionAttribute fa = (AggregateFunctionAttribute) at;
    
                        template = formatTemplate("{} %s {} && {} %s {}", 
                                   r.includeLower() ? "<=" : "<",
                                   r.includeUpper() ? "<=" : "<");
    
                        params = paramsBuilder().variable(lower)
                                .agg(fa.functionId(), fa.propertyPath())
                                .agg(fa.functionId(), fa.propertyPath())
                                .variable(upper)
                                .build();
    
                    }
                    aggFilter = new AggFilter(((NamedExpression) r.value()).id().toString(), new ScriptTemplate(template, params, DataTypes.BOOLEAN));
                }
                //
                // WHERE
                // 
                else {
                    // typical range
                    if (at instanceof FieldAttribute) {
                        RangeQuery rangeQuery = new RangeQuery(r.location(), nameOf(r.value()), 
                                valueOf(r.lower()), r.includeLower(), valueOf(r.upper()), r.includeUpper(), dateFormat(r.value()));
                        query = wrapIfNested(rangeQuery, r.value());
                    }
                }
                
                return new QueryTranslation(query, aggFilter);
            }
            else {
                throw new UnsupportedOperationException("No idea how to translate " + e);
            }
    
        }
    }
    
    
    //
    // Agg translators
    //
    
    static class DistinctCounts extends SingleValueAggTranslator<Count> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, Count c) {
            if (!c.distinct()) {
                return null;
            }
            return new CardinalityAgg(id, path, field(c));
        }
    }
    
    static class Sums extends SingleValueAggTranslator<Sum> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, Sum s) {
            return new SumAgg(id, path, field(s));
        }
    }
    
    static class Avgs extends SingleValueAggTranslator<Avg> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, Avg a) {
            return new AvgAgg(id, path, field(a));
        }
    }
    
    static class Maxes extends SingleValueAggTranslator<Max> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, Max m) {
            return new MaxAgg(id, path, field(m));
        }
    }
    
    static class Mins extends SingleValueAggTranslator<Min> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, Min m) {
            return new MinAgg(id, path, field(m));
        }
    }
    
    static class StatsAggs extends CompoundAggTranslator<Stats> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, Stats s) {
            return new StatsAgg(id, path, field(s));
        }
    }
    
    static class ExtendedStatsAggs extends CompoundAggTranslator<ExtendedStats> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, ExtendedStats e) {
            return new ExtendedStatsAgg(id, path, field(e));
        }
    }
    
    static class MatrixStatsAggs extends CompoundAggTranslator<MatrixStats> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, MatrixStats m) {
            return new MatrixStatsAgg(id, path, singletonList(field(m)));
        }
    }
    
    static class PercentilesAggs extends CompoundAggTranslator<Percentiles> {

        @Override
        protected LeafAgg toAgg(String id, String path, Percentiles p) {
            return new PercentilesAgg(id, path, field(p), doubleValuesOf(p.percents()));
        }
    }

    static class PercentileRanksAggs extends CompoundAggTranslator<PercentileRanks> {

        @Override
        protected LeafAgg toAgg(String id, String path, PercentileRanks p) {
            return new PercentileRanksAgg(id, path, field(p), doubleValuesOf(p.values()));
        }
    }

    static class DateTimes extends SingleValueAggTranslator<Min> {
    
        @Override
        protected LeafAgg toAgg(String id, String path, Min m) {
            return new MinAgg(id, path, field(m));
        }
    }
    
    abstract static class AggTranslator<F extends Function> {
    
        private final Class<F> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());
    
        @SuppressWarnings("unchecked")
        public final LeafAgg apply(String id, String parent, Function f) {
            return (typeToken.isInstance(f) ? asAgg(id, parent, (F) f) : null);
        }
    
        protected abstract LeafAgg asAgg(String id, String parent, F f);
    }
    
    abstract static class SingleValueAggTranslator<F extends Function> extends AggTranslator<F> {
    
        @Override
        protected final LeafAgg asAgg(String id, String parent, F function) {
            String path = parent == null ? id : AggPath.path(parent, id);
            return toAgg(id, AggPath.metricValue(path), function);
        }
    
        protected abstract LeafAgg toAgg(String id, String path, F f);
    }
    
    abstract static class CompoundAggTranslator<C extends CompoundNumericAggregate> extends AggTranslator<C> {
    
        @Override
        protected final LeafAgg asAgg(String id, String parent, C function) {
            String path = parent == null ? id : AggPath.path(parent, id);
            return toAgg(id, path, function);
        }
    
        protected abstract LeafAgg toAgg(String id, String path, C f);
    }
    
    
    abstract static class ExppressionTranslator<E extends Expression> {
    
        private final Class<E> typeToken = ReflectionUtils.detectSuperTypeForRuleLike(getClass());
    
        @SuppressWarnings("unchecked")
        public QueryTranslation translate(Expression exp, boolean onAggs) {
            return (typeToken.isInstance(exp) ? asQuery((E) exp, onAggs) : null);
        }
    
        protected abstract QueryTranslation asQuery(E e, boolean onAggs);
    
        protected static Query wrapIfNested(Query query, Expression exp) {
            if (exp instanceof NestedFieldAttribute) {
                NestedFieldAttribute nfa = (NestedFieldAttribute) exp;
                return new NestedQuery(nfa.location(), nfa.parentPath(), query);
            }
            return query;
        }
    }
}