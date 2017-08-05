/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.analysis.UnknownFunctionException;
import org.elasticsearch.xpack.sql.analysis.UnknownIndexException;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier.Failure;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.AttributeSet;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.ExpressionIdGenerator;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.NestedFieldAttribute;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.SubQueryExpression;
import org.elasticsearch.xpack.sql.expression.TypedAttribute;
import org.elasticsearch.xpack.sql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.UnresolvedStar;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.plan.logical.Aggregate;
import org.elasticsearch.xpack.sql.plan.logical.CatalogTable;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.Join;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.OrderBy;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.sql.plan.logical.With;
import org.elasticsearch.xpack.sql.rule.Rule;
import org.elasticsearch.xpack.sql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Node;
import org.elasticsearch.xpack.sql.type.CompoundDataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;

public class Analyzer extends RuleExecutor<LogicalPlan> {

    private final Catalog catalog;
    private final FunctionRegistry functionRegistry;

    public Analyzer(Catalog catalog, FunctionRegistry functionRegistry) {
        this.catalog = catalog;
        this.functionRegistry = functionRegistry;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch substitution = new Batch("Substitution",
                new CTESubstitution());
        Batch resolution = new Batch("Resolution",
                new ResolveTable(),
                new ResolveRefs(),
                new ResolveOrdinalInOrderByAndGroupBy(),
                new ResolveMissingRefs(),
                new ResolveFunctions(),
                new ResolveAliases(),
                new ProjectedAggregations(), 
                new ResolveAggsInHavingAndOrderBy()
                );
        // TODO: this might be removed since the deduplication happens already in ResolveFunctions
        Batch deduplication = new Batch("Deduplication", 
                new PruneDuplicateFunctions());
        
        return Arrays.asList(substitution, resolution);
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return analyze(plan, true);
    }

    public LogicalPlan analyze(LogicalPlan plan, boolean verify) {
        if (plan.analyzed()) {
            return plan;
        }
        return verify ? verify(execute(plan)) : execute(plan);
    }

    public ExecutionInfo debugAnalyze(LogicalPlan plan) {
        return plan.analyzed() ? null : executeWithInfo(plan);
    }

    public LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = Verifier.verify(plan);
        if (!failures.isEmpty()) {
            throw new VerificationException(failures);
        }
        return plan;
    }
    
    public Map<Node<?>, String> verifyFailures(LogicalPlan plan) {
        Collection<Failure> failures = Verifier.verify(plan);
        return failures.stream().collect(toMap(Failure::source, Failure::message));
    }

    @SuppressWarnings("unchecked")
    private static <E extends Expression> E resolveExpression(E expression, LogicalPlan plan, boolean lenient) {
        return (E) expression.transformUp(e -> {
            if (e instanceof UnresolvedAttribute) {
                UnresolvedAttribute ua = (UnresolvedAttribute) e;
                Attribute a = resolveAgainstList(ua, plan.output(), lenient);
                return (a != null ? a : e);
            }
            return e;
        });
    }
    
    //
    // Shared methods around the analyzer rules
    //

    private static Attribute resolveAgainstList(UnresolvedAttribute u, List<Attribute> attrList, boolean lenient) {
        List<Attribute> matches = new ArrayList<>();
        
        // use the qualifier if present
        if (u.qualifier() != null) {
            for (Attribute attribute : attrList) {
                if (!attribute.synthetic()) {
                    if (Objects.equals(u.qualifiedName(), attribute.qualifiedName())) {
                        matches.add(attribute);
                    }
                    if (attribute instanceof NestedFieldAttribute) {
                        // since u might be unqualified but the parent shows up as a qualifier
                        if (Objects.equals(u.qualifiedName(), attribute.name())) {
                            matches.add(attribute);
                        }
                    }
                }
            }
        }

        // if none is found, try to do a match just on the name (to filter out missing qualifiers)
        if (matches.isEmpty()) {
            for (Attribute attribute : attrList) {
                if (!attribute.synthetic() && Objects.equals(u.name(), attribute.name())) {
                    matches.add(attribute);
                }
            }
        }

        // none found
        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1) {
            return matches.get(0);
        }

        // too many references - should it be ignored?
        if (!lenient) {
            throw new AnalysisException(u, "Reference %s is ambiguous, matches any of %s", u.nodeString(), matches);
        }

        return null;
    }

    private static boolean hasStar(List<? extends Expression> exprs) {
        for (Expression expression : exprs) {
            if (expression instanceof UnresolvedStar) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean containsAggregate(List<? extends Expression> list) {
        return Expressions.anyMatchInList(list, Functions::isAggregateFunction);
    }

    private static boolean containsAggregate(Expression exp) {
        return containsAggregate(singletonList(exp));
    }


    private class CTESubstitution extends AnalyzeRule<With> {

        @Override
        protected LogicalPlan rule(With plan) {
            return substituteCTE(plan.child(), plan.subQueries());
        }

        private LogicalPlan substituteCTE(LogicalPlan p, Map<String, SubQueryAlias> subQueries) {
            if (p instanceof UnresolvedRelation) {
                UnresolvedRelation ur = (UnresolvedRelation) p;
                SubQueryAlias subQueryAlias = subQueries.get(ur.table().index());
                if (subQueryAlias != null) {
                    if (ur.alias() != null) {
                        return new SubQueryAlias(ur.location(), subQueryAlias, ur.alias());
                    }
                    return subQueryAlias;
                }
                return ur;
            }
            return p.transformExpressionsDown(e -> {
                if (e instanceof SubQueryExpression) {
                    SubQueryExpression sq = (SubQueryExpression) e;
                    return sq.withQuery(substituteCTE(sq.query(), subQueries));
                }
                return e;
            });
        }
    }

    private class ResolveTable extends AnalyzeRule<UnresolvedRelation> {
        @Override
        protected LogicalPlan rule(UnresolvedRelation plan) {
            TableIdentifier table = plan.table();
            EsIndex found;
            try {
                found = catalog.getIndex(table.index());
            } catch (SqlIllegalArgumentException e) {
                throw new AnalysisException(plan, e.getMessage(), e);
            }
            if (found == null) {
                throw new UnknownIndexException(table.index(), plan);
            }

            LogicalPlan catalogTable = new CatalogTable(plan.location(), found);
            SubQueryAlias sa = new SubQueryAlias(plan.location(), catalogTable, table.index());

            if (plan.alias() != null) {
                sa = new SubQueryAlias(plan.location(), sa, plan.alias());
            }

            return sa;
        }
    }

    private class ResolveRefs extends AnalyzeRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            // if the children are not resolved, there's no way the node can be resolved
            if (!plan.childrenResolved()) {
                return plan;
            }

            // okay, there's a chance so let's get started

            if (plan instanceof Project) {
                Project p = (Project) plan;
                if (hasStar(p.projections())) {
                    return new Project(p.location(), p.child(), expandProjections(p.projections(), p.child()));
                }
            }
            else if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                if (hasStar(a.aggregates())) {
                    return new Aggregate(a.location(), a.child(), a.groupings(),
                            expandProjections(a.aggregates(), a.child()));
                }
                // if the grouping is unresolved but the aggs are, use the latter to resolve the former
                // solves the case of queries declaring an alias in SELECT and referring to it in GROUP BY
                if (!a.expressionsResolved() && Resolvables.resolved(a.aggregates())) {
                    List<Expression> groupings = a.groupings();
                    List<Expression> newGroupings = new ArrayList<>();
                    List<Attribute> resolved = Expressions.asAttributes(a.aggregates());
                    boolean changed = false;
                    for (int i = 0; i < groupings.size(); i++) {
                        Expression grouping = groupings.get(i);
                        if (grouping instanceof UnresolvedAttribute) {
                            Attribute maybeResolved = resolveAgainstList((UnresolvedAttribute) grouping, resolved, true);
                            if (maybeResolved != null) {
                                changed = true;
                                // use the matched expression (not its attribute)
                                grouping = a.aggregates().get(i);
                            }
                        }
                        newGroupings.add(grouping);
                    }

                    return changed ? new Aggregate(a.location(), a.child(), newGroupings, a.aggregates()) : a;
                }
            }

            else if (plan instanceof Join) {
                Join j = (Join) plan;
                if (!j.duplicatesResolved()) {
                    LogicalPlan deduped = dedupRight(j.left(), j.right());
                    return new Join(j.location(), j.left(), deduped, j.type(), j.condition());
                }
            }
            // try resolving the order expression (the children are resolved as this point)
            else if (plan instanceof OrderBy) {
                OrderBy o = (OrderBy) plan;
                if (!o.resolved()) {
                    List<Order> resolvedOrder = o.order().stream()
                            .map(or -> resolveExpression(or, o.child(), true))
                            .collect(toList());
                    return new OrderBy(o.location(), o.child(), resolvedOrder);
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Attempting to resolve {}", plan.nodeString());
            }

            return plan.transformExpressionsUp(e -> {
                if (e instanceof UnresolvedAttribute) {
                    UnresolvedAttribute u = (UnresolvedAttribute) e;
                    NamedExpression named = resolveAgainstList(u,
                            plan.children().stream()
                              .flatMap(c -> c.output().stream())
                              .collect(toList()),
                            false);
                    // if resolved, return it; otherwise keep it in place to be resolved later
                    if (named != null) {
                        // it's a compound type so convert it 
                        if (named instanceof TypedAttribute && ((TypedAttribute) named).dataType() instanceof CompoundDataType) {
                            named = new UnresolvedStar(e.location(),
                                    new UnresolvedAttribute(e.location(), u.name(), u.qualifier()));
                        }
                        
                        if (log.isTraceEnabled()) {
                            log.trace("Resolved {} to {}", u, named);
                        }
                        return named;
                    }
                }
                //TODO: likely have to expand * inside functions as well
                return e;
            });
        }

        private List<NamedExpression> expandProjections(List<? extends NamedExpression> projections, LogicalPlan child) {
            return projections.stream().flatMap(e -> {
                // check if there's a qualifier
                // no - means only top-level
                // it is - return only that level
                    if (e instanceof UnresolvedStar) {
                        List<Attribute> output = child.output();
                        UnresolvedStar us = (UnresolvedStar) e;

                        Stream<Attribute> stream = output.stream();

                        if (us.qualifier() == null) {
                            stream = stream.filter(a -> !(a instanceof NestedFieldAttribute));
                        }
                        
                        // if there's a qualifier, inspect that level
                        if (us.qualifier() != null) {
                            // qualifier is selected, need to resolve that first.
                            Attribute qualifier = resolveAgainstList(us.qualifier(), output, false);
                            stream = stream.filter(a -> (a instanceof NestedFieldAttribute) 
                                              && Objects.equals(a.qualifier(), qualifier.qualifier())
                                              && Objects.equals(((NestedFieldAttribute) a).parentPath(), qualifier.name()));
                        }
                        
                        return stream.filter(a -> !(a.dataType() instanceof CompoundDataType));
                    }
                    else if (e instanceof UnresolvedAlias) {
                        UnresolvedAlias ua = (UnresolvedAlias) e;
                        if (ua.child() instanceof UnresolvedStar) {
                            return child.output().stream();
                        }
                        return Stream.of(e);
                    }
                    return Stream.of(e);
                })
                .map(NamedExpression.class::cast)
                .collect(toList());
        }

        // generate a new (right) logical plan with different IDs for all conflicting attributes
        private LogicalPlan dedupRight(LogicalPlan left, LogicalPlan right) {
            AttributeSet conflicting = left.outputSet().intersect(right.outputSet());

            if (log.isTraceEnabled()) {
                log.trace("Trying to resolve conflicts {} between left {} and right {}", conflicting, left.nodeString(), right.nodeString());
            }

            throw new UnsupportedOperationException("don't know how to resolve conficting IDs yet");
        }
    }

    // Allow ordinal positioning in order/sort by (quite useful when dealing with aggs)
    // Note that ordering starts at 1
    private class ResolveOrdinalInOrderByAndGroupBy extends AnalyzeRule<LogicalPlan> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (!plan.childrenResolved()) {
                return plan;
            }
            if (plan instanceof OrderBy) {
                OrderBy orderBy = (OrderBy) plan;
                boolean changed = false;
                
                List<Order> newOrder = new ArrayList<>(orderBy.order().size());
                List<Attribute> ordinalReference = orderBy.child().output();
                int max = ordinalReference.size();
                
                for (Order order : orderBy.order()) {
                    Integer ordinal = findOrdinal(order.child());
                    if (ordinal != null) {
                        changed = true;
                        if (ordinal > 0 && ordinal <= max) {
                            newOrder.add(new Order(order.location(), orderBy.child().output().get(ordinal - 1), order.direction()));
                        }
                        else {
                            throw new AnalysisException(order, "Invalid %d specified in OrderBy (valid range is [1, %d])", ordinal, max);
                        }
                    }
                    else {
                        newOrder.add(order);
                    }
                }
                
                return changed ? new OrderBy(orderBy.location(), orderBy.child(), newOrder) : orderBy;
            }
            
            if (plan instanceof Aggregate) {
                Aggregate agg = (Aggregate) plan;
                
                if (!Resolvables.resolved(agg.aggregates())) {
                    return agg;
                }
                
                boolean changed = false;
                List<Expression> newGroupings = new ArrayList<>(agg.groupings().size());
                List<? extends NamedExpression> aggregates = agg.aggregates();
                int max = aggregates.size();
                
                for (Expression exp : agg.groupings()) {
                    Integer ordinal = findOrdinal(exp);
                    if (ordinal != null) {
                        changed = true;
                        if (ordinal > 0 && ordinal <= max) {
                            NamedExpression reference = aggregates.get(ordinal);
                            if (containsAggregate(reference)) {
                                throw new AnalysisException(exp, "Group ordinal %d refers to an aggregate function %s which is not compatible/allowed with GROUP BY", ordinal, reference.nodeName());
                            }
                            newGroupings.add(reference);
                        }
                        else {
                            throw new AnalysisException(exp, "Invalid ordinal %d specified in Aggregate (valid range is [1, %d])", ordinal, max);
                        }
                    }
                    else {
                        newGroupings.add(exp);
                    }
                }
                
                return changed ? new Aggregate(agg.location(), agg.child(), newGroupings, aggregates) : agg;
            }

            return plan;
        }

        private Integer findOrdinal(Expression expression) {
            if (expression instanceof Literal) {
                Literal l = (Literal) expression;
                if (l.dataType().isInteger()) {
                    Object v = l.value();
                    if (v instanceof Number) {
                        return Integer.valueOf(((Number) v).intValue());
                    }
                }
            }
            return null;
        }
    }

    // In some SQL dialects it is valid to filter or sort by attributes not present in the SELECT clause.
    // As such this rule is an extended version of ResolveRefs
    private class ResolveMissingRefs extends AnalyzeRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan instanceof OrderBy && !plan.resolved() && plan.childrenResolved()) {
                OrderBy o = (OrderBy) plan;
                List<Order> maybeResolved = o.order().stream()
                        .map(or -> tryResolveExpression(or, o.child()))
                        .collect(toList());

                AttributeSet resolvedRefs = Expressions.references(maybeResolved.stream()
                    .filter(Expression::resolved)
                    .collect(toList()));


                AttributeSet missing = resolvedRefs.substract(o.child().outputSet());
                if (!missing.isEmpty()) {
                    // Add missing attributes but project them away afterwards
                    return new Project(o.location(), 
                            new OrderBy(o.location(), propagateMissing(o.child(), missing), maybeResolved),
                            o.child().output());
                }
                if (!maybeResolved.equals(o.order())) {
                    return new OrderBy(o.location(), o.child(), maybeResolved);
                }
            }

            if (plan instanceof Filter && !plan.resolved() && plan.childrenResolved()) {
                Filter f = (Filter) plan;
                Expression maybeResolved = tryResolveExpression(f.condition(), f.child());
                AttributeSet resolvedRefs = new AttributeSet(maybeResolved.references().stream()
                    .filter(Expression::resolved)
                    .collect(toList()));
                
                AttributeSet missing = resolvedRefs.substract(f.child().outputSet());
                if (!missing.isEmpty()) {
                    // Again, add missing attributes and project them away
                    return new Project(f.location(),
                            new Filter(f.location(), propagateMissing(f.child(), missing), maybeResolved),
                            f.child().output());
                }
                if (!maybeResolved.equals(f.condition())) {
                    return new Filter(f.location(), f.child(), maybeResolved);
                }
            }

            return plan;
        }

        private <E extends Expression> E tryResolveExpression(E exp, LogicalPlan plan) {
            E resolved = resolveExpression(exp, plan, true);
            if (!resolved.resolved()) {
                // look at unary trees but ignore subqueries
                if (plan.children().size() == 1 && !(plan instanceof SubQueryAlias)) {
                    return tryResolveExpression(resolved, plan.children().get(0));
                }
            }
            return resolved;
        }
        

        private LogicalPlan propagateMissing(LogicalPlan logicalPlan, AttributeSet missing) {
            // no more attributes, bail out
            if (missing.isEmpty()) {
                return logicalPlan;
            }

            return logicalPlan.transformDown(plan -> {
                if (plan instanceof Project) {
                    Project p = (Project) plan;
                    AttributeSet diff = missing.substract(p.child().outputSet());
                    return new Project(p.location(), propagateMissing(p.child(), diff), combine(p.projections(), missing));
                }

                if (plan instanceof Aggregate) {
                    Aggregate a = (Aggregate) plan;
                    // missing attributes can only be grouping expressions
                    for (Attribute m : missing) {
                        // but we don't can't add an agg if the group is missing
                        if (!Expressions.anyMatchInList(a.groupings(), g -> g.canonicalEquals(m))) {
                            // we cannot propagate the missing attribute, bail out
                            //throw new AnalysisException(logicalPlan, "Cannot add missing attribute %s to %s", m.name(), plan);
                            return plan;
                        }
                    }
                    return new Aggregate(a.location(), a.child(), a.groupings(), combine(a.aggregates(), missing));
                }

                return plan;
            });
        }
    }

    // to avoid creating duplicate functions
    // this rule does two iterations
    // 1. collect all functions
    // 2. search unresolved functions and first try resolving them from already 'seen' functions
    private class ResolveFunctions extends AnalyzeRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            Map<String, List<Function>> seen = new LinkedHashMap<>();
            // collect (and replace duplicates)
            LogicalPlan p = plan.transformExpressionsUp(e -> collectResolvedAndReplace(e, seen));
            // resolve based on seen
            return resolve(p, seen);
        }

        private Expression collectResolvedAndReplace(Expression e, Map<String, List<Function>> seen) {
            if (e instanceof Function && e.resolved()) {
                Function f = (Function) e;
                String fName = f.functionName();
                // the function is resolved and its name normalized already
                List<Function> list = getList(seen, fName);
                for (Function seenFunction : list) {
                    if (seenFunction != f && f.arguments().equals(seenFunction.arguments())) {
                        return seenFunction;
                    }
                }
                list.add(f);
            }

            return e;
        }

        protected LogicalPlan resolve(LogicalPlan plan, Map<String, List<Function>> seen) {
            return plan.transformExpressionsUp(e -> {
                if (e instanceof UnresolvedFunction) {
                    UnresolvedFunction uf = (UnresolvedFunction) e;
                    String name = uf.name();

                    if (hasStar(uf.arguments())) {
                        if (uf.distinct()) {
                            throw new AnalysisException(uf, "DISTINCT and wildcard/star are not compatible");
                        }
                        // TODO: might be removed
                        // dedicated count optimization
                        if (name.toUpperCase(Locale.ROOT).equals("COUNT")) {
                            uf = new UnresolvedFunction(uf.location(), uf.name(), uf.distinct(), singletonList(Literal.of(uf.arguments().get(0).location(), Integer.valueOf(1))));
                        }
                    }

                    if (!uf.childrenResolved()) {
                        return uf;
                    }

                    // need to normalize the name for the lookup
                    List<Function> list = getList(seen, StringUtils.camelCaseToUnderscore(name));
                    // first try to resolve from seen functions
                    if (!list.isEmpty()) {
                        for (Function seenFunction : list) {
                            if (uf.arguments().equals(seenFunction.arguments())) {
                                return seenFunction;
                            }
                        }
                    }

                    // not seen before, use the registry
                    if (!functionRegistry.functionExists(name)) {
                        throw new UnknownFunctionException(name, uf);
                    }
                    // TODO: look into Generator for significant terms, etc..
                    Function f = functionRegistry.resolveFunction(uf, SqlSession.CURRENT.get());

                    list.add(f);
                    return f;
                }
                return e;
            });
        }

        private List<Function> getList(Map<String, List<Function>> seen, String name) {
            List<Function> list = seen.get(name);
            if (list == null) {
                list = new ArrayList<Function>();
                seen.put(name, list);
            }
            return list;
        }
    }

    private class ResolveAliases extends AnalyzeRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan instanceof Project) {
                Project p = (Project) plan;
                if (p.childrenResolved() && hasUnresolvedAliases(p.projections())) {
                    return new Project(p.location(), p.child(), assignAliases(p.projections()));
                }
                return p;
            }
            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                if (a.childrenResolved() && hasUnresolvedAliases(a.aggregates())) {
                    return new Aggregate(a.location(), a.child(), a.groupings(), assignAliases(a.aggregates()));
                }
                return a;
            }

            return plan;
        }

        private boolean hasUnresolvedAliases(List<? extends NamedExpression> expressions) {
            return (expressions != null && expressions.stream().anyMatch(e -> e instanceof UnresolvedAlias));
        }

        private List<NamedExpression> assignAliases(List<? extends NamedExpression> exprs) {
            List<NamedExpression> newExpr = new ArrayList<>(exprs.size());
            for (int i = 0; i < exprs.size(); i++) {
                NamedExpression expr = exprs.get(i);
                NamedExpression transformed = (NamedExpression) expr.transformUp(ua -> {
                    Expression child = ua.child();
                    if (child instanceof NamedExpression) {
                        return child;
                    }
                    if (!child.resolved()) {
                        return ua;
                    }
                    if (child instanceof Cast) {
                        Cast c = (Cast) child;
                        if (c.argument() instanceof NamedExpression) {
                            return new Alias(c.location(), ((NamedExpression) c.argument()).name(), c);
                        }
                    }
                    //TODO: maybe add something closer to SQL
                    return new Alias(child.location(), child.toString(), child);
                }, UnresolvedAlias.class);
                newExpr.add(expr.equals(transformed) ? expr : transformed);
            }
            return newExpr;
        }
    }
    

    //
    // Replace a project with aggregation into an aggregation
    //
    private class ProjectedAggregations extends AnalyzeRule<Project> {

        @Override
        protected LogicalPlan rule(Project p) {
            if (containsAggregate(p.projections())) {
                return new Aggregate(p.location(), p.child(), emptyList(), p.projections());
            }
            return p;
        }
    };

    //
    // Handle aggs in HAVING and ORDER BY clause. In both cases, the function argument
    // cannot be resolved being masked by the subplan; to get around it the expression is pushed down
    // and then projected.
    //
    private class ResolveAggsInHavingAndOrderBy extends AnalyzeRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            // HAVING = Filter followed by an Agg
            if (plan instanceof Filter) {
                Filter f = (Filter) plan;
                if (f.child() instanceof Aggregate && f.child().resolved()) {
                    Aggregate agg = (Aggregate) f.child();

                    // first try to see whether the agg contains the functions in the filter
                    // and if so point the aliases to them

                    Map<Function, Attribute> resolvedFunc = new LinkedHashMap<>();
                    for (NamedExpression ne : agg.aggregates()) {
                        if (ne instanceof Alias) {
                            Alias as = (Alias) ne;
                            if (as.child() instanceof Function) {
                                resolvedFunc.put((Function) as.child(), as.toAttribute());
                            }
                        }
                    }

                    if (resolvedFunc.isEmpty()) {
                        return plan;
                    }
                    
                    Expression resolvedCondition = f.condition().transformUp(exp -> {
                        if (!(exp instanceof Function)) {
                            return exp;
                        }
                        
                        Function func = (Function) exp;
                        Function match = null;
                        if (!func.resolved()) {
                            // if it not resolved try to resolve the arguments
                            match = resolveFunction(func, resolvedFunc.keySet());
                        }
                        else {
                            // make sure to eliminate total count (it does not make sense to condition by it or does it?)
                            if (isTotalCount(func)) {
                                throw new AnalysisException(f, "Global/Total count cannot be used inside the HAVING clause");
                            }
                            // if it is resolved, find its equal
                            match = findResolved(func, resolvedFunc.keySet());
                        }
                        
                        return match != null ? resolvedFunc.get(match) : exp;
                    });

                    if (resolvedCondition != f.condition()) {
                        return new Filter(f.location(), f.child(), resolvedCondition);
                    }
                    // through an exception instead of full analysis
                }
                return plan;
            }

            return plan;
        }
        
        private boolean isTotalCount(Function f) {
            if (f instanceof Count) {
                Count c = (Count) f;
                if (!c.distinct()) {
                    if (c.field() instanceof Literal && c.field().dataType().isInteger()) {
                        return true;
                    }
                }
            }
            return false;
        }
        
        private Function resolveFunction(Function func, Collection<Function> resolvedFunc) {
            for (Function rf : resolvedFunc) {
                if (rf.name().equals(func.name()) && rf.arguments().size() == func.arguments().size()) {
                    for (int i = 0; i < func.arguments().size(); i++) {
                        Expression arg = func.arguments().get(i);
                        Expression resolvedArg = rf.arguments().get(i);
                        
                        // try to resolve the arg based on the function
                        if (arg instanceof UnresolvedAttribute && resolvedArg instanceof NamedExpression) {
                            UnresolvedAttribute u = (UnresolvedAttribute) arg;
                            NamedExpression named = resolveAgainstList(u,
                                    singletonList(((NamedExpression) resolvedArg).toAttribute()),
                                    true);

                            if (named == null) {
                                break;
                            }
                        }
                        else {
                            if (!arg.canonicalEquals(rf)) {
                                break;
                            }
                        }
                    }
                    // found a match
                    return rf;
                }
            }
            return null;
        }

        private Function findResolved(Function func, Collection<Function> resolvedFunc) {
            for (Function f : resolvedFunc) {
                if (f.canonicalEquals(func)) {
                    return f;
                }
            }
            return null;
        }

        // rule to add an agg from a filter to the aggregate below it
        // suitable when doing filtering with extra aggs:
        // SELECT SUM(g) FROM ... GROUP BY g HAVING AVG(g) > 10
        // AVG is used for filtering but it's not declared on the Aggregate
        private LogicalPlan fullAnalysis(Filter f, Aggregate agg) {
            try {
                // try to resolve the expression separately

                // first, wrap it in an alias 
                ExpressionId newId = ExpressionIdGenerator.newId();
                Alias wrappedCondition = new Alias(f.location(), "generated#" + newId, null, f.condition(), newId, true);
                // pass it to an Agg 
                Aggregate attempt = new Aggregate(agg.location(), agg.child(), agg.groupings(), singletonList(wrappedCondition));
                // then try resolving it
                Aggregate analyzedAgg = (Aggregate) execute(attempt);
                NamedExpression analyzedCondition = analyzedAgg.aggregates().get(0);
                
                // everything was resolved
                if (analyzedCondition.resolved()) {
                    List<NamedExpression> resolvedAggExp = new ArrayList<>();

                    // break down the expression into parts
                    Expression analyzedFilterCondition = analyzedCondition.transformDown(exp -> {
                        NamedExpression named = null;
                        
                        if (exp instanceof Function) {
                            Function func = (Function) exp;
                            named = new Alias(func.location(), func.name(), func);
                        }
                        
                        // if the grouping appears in the filter (and it is not exported out)
                        if (Expressions.anyMatchInList(agg.groupings(), g -> g.canonicalEquals(exp)) 
                                && !Expressions.anyMatchInList(agg.output(), o -> o.canonicalEquals(exp))) {
                             named = exp instanceof NamedExpression ? (NamedExpression) exp : new Alias(exp.location(), exp.nodeName(), exp);
                        }

                        if (named != null) {
                            resolvedAggExp.add(named);
                            return named.toAttribute();
                        }

                        return exp;
                    });
                    
                    // Replace the resolved expression 
                    if (!resolvedAggExp.isEmpty()) {
                        // push them down to the agg
                        Aggregate newAgg = new Aggregate(agg.location(), agg.child(), agg.groupings(),
                                combine(agg.aggregates(), resolvedAggExp));
                        // wire it up to the filter with the new condition
                        Filter newFilter = new Filter(f.location(), newAgg, analyzedFilterCondition);
                        // and finally project the fluff away
                        return new Project(f.location(), newFilter, agg.output());
                    }
                }
                
            } catch (AnalysisException ex) {
            }

            return f;
        }
    }

    private class PruneDuplicateFunctions extends AnalyzeRule<LogicalPlan> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        public LogicalPlan rule(LogicalPlan plan) {
            List<Function> seen = new ArrayList<>();
            LogicalPlan p = plan.transformExpressionsUp(e -> rule(e, seen));
            return p;
        }

        private Expression rule(Expression e, List<Function> seen) {
            if (e instanceof Function) {
                Function f = (Function) e;
                for (Function seenFunction : seen) {
                    if (seenFunction != f && functionsEquals(f, seenFunction)) {
                        return seenFunction;
                    }
                }
                seen.add(f);
            }

            return e;
        }

        private boolean functionsEquals(Function f, Function seenFunction) {
            return f.name().equals(seenFunction.name()) && f.arguments().equals(seenFunction.arguments());
        }
    }

    abstract static class AnalyzeRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(t -> t.analyzed() || (skipResolved() && t.resolved()) ? t : rule(t), typeToken());
        }

        @Override
        protected abstract LogicalPlan rule(SubPlan plan);

        protected boolean skipResolved() {
            return true;
        }
    }
}