/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.xpack.sql.analysis.AnalysisException;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier.Failure;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.AttributeMap;
import org.elasticsearch.xpack.sql.expression.AttributeSet;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Order;
import org.elasticsearch.xpack.sql.expression.SubQueryExpression;
import org.elasticsearch.xpack.sql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.sql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.sql.expression.UnresolvedStar;
import org.elasticsearch.xpack.sql.expression.function.Function;
import org.elasticsearch.xpack.sql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.expression.function.Functions;
import org.elasticsearch.xpack.sql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.plan.logical.Aggregate;
import org.elasticsearch.xpack.sql.plan.logical.EsRelation;
import org.elasticsearch.xpack.sql.plan.logical.Filter;
import org.elasticsearch.xpack.sql.plan.logical.Join;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.sql.plan.logical.OrderBy;
import org.elasticsearch.xpack.sql.plan.logical.Project;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.sql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.sql.plan.logical.With;
import org.elasticsearch.xpack.sql.rule.Rule;
import org.elasticsearch.xpack.sql.rule.RuleExecutor;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.InvalidMappedField;
import org.elasticsearch.xpack.sql.type.UnsupportedEsField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;

public class Analyzer extends RuleExecutor<LogicalPlan> {
    /**
     * Valid functions.
     */
    private final FunctionRegistry functionRegistry;
    /**
     * Information about the index against which the SQL is being analyzed.
     */
    private final IndexResolution indexResolution;
    /**
     * Per-request specific settings needed in some of the functions (timezone, username and clustername),
     * to which they are attached.
     */
    private final Configuration configuration;
    /**
     * The verifier has the role of checking the analyzed tree for failures and build a list of failures.
     */
    private final Verifier verifier;

    public Analyzer(Configuration configuration, FunctionRegistry functionRegistry, IndexResolution results, Verifier verifier) {
        this.configuration = configuration;
        this.functionRegistry = functionRegistry;
        this.indexResolution = results;
        this.verifier = verifier;
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
                new ResolveAggsInHaving()
                //new ImplicitCasting()
                );
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
        Collection<Failure> failures = verifier.verify(plan);
        if (!failures.isEmpty()) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Expression> E resolveExpression(E expression, LogicalPlan plan) {
        return (E) expression.transformUp(e -> {
            if (e instanceof UnresolvedAttribute) {
                UnresolvedAttribute ua = (UnresolvedAttribute) e;
                Attribute a = resolveAgainstList(ua, plan.output());
                return a != null ? a : e;
            }
            return e;
        });
    }

    //
    // Shared methods around the analyzer rules
    //
    private static Attribute resolveAgainstList(UnresolvedAttribute u, Collection<Attribute> attrList) {
        return resolveAgainstList(u, attrList, false);
    }

    private static Attribute resolveAgainstList(UnresolvedAttribute u, Collection<Attribute> attrList, boolean allowCompound) {
        List<Attribute> matches = new ArrayList<>();

        // first take into account the qualified version
        boolean qualified = u.qualifier() != null;

        for (Attribute attribute : attrList) {
            if (!attribute.synthetic()) {
                boolean match = qualified ?
                        Objects.equals(u.qualifiedName(), attribute.qualifiedName()) :
                        // if the field is unqualified
                        // first check the names directly
                        (Objects.equals(u.name(), attribute.name())
                             // but also if the qualifier might not be quoted and if there's any ambiguity with nested fields
                             || Objects.equals(u.name(), attribute.qualifiedName()));
                if (match) {
                    matches.add(attribute.withLocation(u.location()));
                }
            }
        }

        // none found
        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1) {
            return handleSpecialFields(u, matches.get(0), allowCompound);
        }

        return u.withUnresolvedMessage("Reference [" + u.qualifiedName()
                + "] is ambiguous (to disambiguate use quotes or qualifiers); matches any of " +
                 matches.stream()
                 .map(a -> "\"" + a.qualifier() + "\".\"" + a.name() + "\"")
                 .sorted()
                 .collect(toList())
                );
    }

    private static Attribute handleSpecialFields(UnresolvedAttribute u, Attribute named, boolean allowCompound) {
        // if it's a object/compound type, keep it unresolved with a nice error message
        if (named instanceof FieldAttribute) {
            FieldAttribute fa = (FieldAttribute) named;

            // incompatible mappings
            if (fa.field() instanceof InvalidMappedField) {
                named = u.withUnresolvedMessage("Cannot use field [" + fa.name() + "] due to ambiguities being "
                        + ((InvalidMappedField) fa.field()).errorMessage());
            }
            // unsupported types
            else if (DataTypes.isUnsupported(fa.dataType())) {
                UnsupportedEsField unsupportedField = (UnsupportedEsField) fa.field();
                named = u.withUnresolvedMessage(
                        "Cannot use field [" + fa.name() + "] type [" + unsupportedField.getOriginalType() + "] as is unsupported");
            }
            // compound fields
            else if (allowCompound == false && fa.dataType().isPrimitive() == false) {
                named = u.withUnresolvedMessage(
                        "Cannot use field [" + fa.name() + "] type [" + fa.dataType().esType + "] only its subfields");
            }
        }
        return named;
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
        return Expressions.anyMatch(list, Functions::isAggregate);
    }

    private static boolean containsAggregate(Expression exp) {
        return containsAggregate(singletonList(exp));
    }

    private static class CTESubstitution extends AnalyzeRule<With> {

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
            // inlined queries (SELECT 1 + 2) are already resolved
            else if (p instanceof LocalRelation) {
                return p;
            }

            return p.transformExpressionsDown(e -> {
                if (e instanceof SubQueryExpression) {
                    SubQueryExpression sq = (SubQueryExpression) e;
                    return sq.withQuery(substituteCTE(sq.query(), subQueries));
                }
                return e;
            });
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }

    private class ResolveTable extends AnalyzeRule<UnresolvedRelation> {
        @Override
        protected LogicalPlan rule(UnresolvedRelation plan) {
            TableIdentifier table = plan.table();
            if (indexResolution.isValid() == false) {
                return plan.unresolvedMessage().equals(indexResolution.toString()) ? plan : new UnresolvedRelation(plan.location(),
                        plan.table(), plan.alias(), indexResolution.toString());
            }
            assert indexResolution.matches(table.index());
            LogicalPlan logicalPlan = new EsRelation(plan.location(), indexResolution.get());
            SubQueryAlias sa = new SubQueryAlias(plan.location(), logicalPlan, table.index());

            if (plan.alias() != null) {
                sa = new SubQueryAlias(plan.location(), sa, plan.alias());
            }

            return sa;
        }
    }

    private static class ResolveRefs extends AnalyzeRule<LogicalPlan> {

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
                    AttributeMap<Expression> resolved = Expressions.asAttributeMap(a.aggregates());
                    boolean changed = false;
                    for (Expression grouping : groupings) {
                        if (grouping instanceof UnresolvedAttribute) {
                            Attribute maybeResolved = resolveAgainstList((UnresolvedAttribute) grouping, resolved.keySet());
                            if (maybeResolved != null) {
                                changed = true;
                                // use the matched expression (not its attribute)
                                grouping = resolved.get(maybeResolved);
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
                            .map(or -> resolveExpression(or, o.child()))
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
                    List<Attribute> childrenOutput = new ArrayList<>();
                    for (LogicalPlan child : plan.children()) {
                        childrenOutput.addAll(child.output());
                    }
                    NamedExpression named = resolveAgainstList(u, childrenOutput);
                    // if resolved, return it; otherwise keep it in place to be resolved later
                    if (named != null) {
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
            List<NamedExpression> result = new ArrayList<>();

            List<Attribute> output = child.output();
            for (NamedExpression ne : projections) {
                if (ne instanceof UnresolvedStar) {
                    List<NamedExpression> expanded = expandStar((UnresolvedStar) ne, output);
                    // the field exists, but cannot be expanded (no sub-fields)
                    if (expanded.isEmpty()) {
                        result.add(ne);
                    } else {
                        result.addAll(expanded);
                    }
                } else if (ne instanceof UnresolvedAlias) {
                    UnresolvedAlias ua = (UnresolvedAlias) ne;
                    if (ua.child() instanceof UnresolvedStar) {
                        result.addAll(expandStar((UnresolvedStar) ua.child(), output));
                    }
                } else {
                    result.add(ne);
                }
            }

            return result;
        }

        private List<NamedExpression> expandStar(UnresolvedStar us, List<Attribute> output) {
            List<NamedExpression> expanded = new ArrayList<>();

            // a qualifier is specified - since this is a star, it should be a CompoundDataType
            if (us.qualifier() != null) {
                // resolve the so-called qualifier first
                // since this is an unresolved start we don't know whether it's a path or an actual qualifier
                Attribute q = resolveAgainstList(us.qualifier(), output, true);

                // the wildcard couldn't be expanded because the field doesn't exist at all
                // so, add to the list of expanded attributes its qualifier (the field without the wildcard)
                // the qualifier will be unresolved and later used in the error message presented to the user
                if (q == null) {
                    return singletonList(us.qualifier());
                }
                // qualifier is unknown (e.g. unsupported type), bail out early
                else if (q.resolved() == false) {
                    return singletonList(q);
                }

                // now use the resolved 'qualifier' to match
                for (Attribute attr : output) {
                    // filter the attributes that match based on their path
                    if (attr instanceof FieldAttribute) {
                        FieldAttribute fa = (FieldAttribute) attr;
                        if (DataTypes.isUnsupported(fa.dataType())) {
                            continue;
                        }
                        if (q.qualifier() != null) {
                            if (Objects.equals(q.qualifiedName(), fa.qualifiedPath())) {
                                expanded.add(fa.withLocation(attr.location()));
                            }
                        } else {
                            // use the path only to match non-compound types
                            if (Objects.equals(q.name(), fa.path())) {
                                expanded.add(fa.withLocation(attr.location()));
                            }
                        }
                    }
                }
            } else {
                // add only primitives
                // but filter out multi fields (allow only the top-level value)
                Set<Attribute> seenMultiFields = new LinkedHashSet<>();

                for (Attribute a : output) {
                    if (!DataTypes.isUnsupported(a.dataType()) && a.dataType().isPrimitive()) {
                        if (a instanceof FieldAttribute) {
                            FieldAttribute fa = (FieldAttribute) a;
                            // skip nested fields and seen multi-fields
                            if (!fa.isNested() && !seenMultiFields.contains(fa.parent())) {
                                expanded.add(a);
                                seenMultiFields.add(a);
                            }
                        } else {
                            expanded.add(a);
                        }
                    }
                }
            }

            return expanded;
        }

        // generate a new (right) logical plan with different IDs for all conflicting attributes
        private LogicalPlan dedupRight(LogicalPlan left, LogicalPlan right) {
            AttributeSet conflicting = left.outputSet().intersect(right.outputSet());

            if (log.isTraceEnabled()) {
                log.trace("Trying to resolve conflicts " + conflicting + " between left " + left.nodeString()
                        + " and right " + right.nodeString());
            }

            throw new UnsupportedOperationException("don't know how to resolve conficting IDs yet");
        }
    }

    // Allow ordinal positioning in order/sort by (quite useful when dealing with aggs)
    // Note that ordering starts at 1
    private static class ResolveOrdinalInOrderByAndGroupBy extends AnalyzeRule<LogicalPlan> {

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
                            newOrder.add(new Order(order.location(), orderBy.child().output().get(ordinal - 1), order.direction(),
                                    order.nullsPosition()));
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
                            NamedExpression reference = aggregates.get(ordinal - 1);
                            if (containsAggregate(reference)) {
                                throw new AnalysisException(exp, "Group ordinal " + ordinal + " refers to an aggregate function "
                                        + reference.nodeName() + " which is not compatible/allowed with GROUP BY");
                            }
                            newGroupings.add(reference);
                        }
                        else {
                            throw new AnalysisException(exp, "Invalid ordinal " + ordinal
                                    + " specified in Aggregate (valid range is [1, " + max + "])");
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

    // It is valid to filter (including HAVING) or sort by attributes not present in the SELECT clause.
    // This rule pushed down the attributes for them to be resolved then projects them away.
    // As such this rule is an extended version of ResolveRefs
    private static class ResolveMissingRefs extends AnalyzeRule<LogicalPlan> {

        private static class AggGroupingFailure {
            final List<String> expectedGrouping;

            private AggGroupingFailure(List<String> expectedGrouping) {
                this.expectedGrouping = expectedGrouping;
            }
        }

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
                    List<Attribute> failedAttrs = new ArrayList<>();
                    LogicalPlan newChild = propagateMissing(o.child(), missing, failedAttrs);

                    // resolution failed and the failed expressions might contain resolution information so copy it over
                    if (!failedAttrs.isEmpty()) {
                        List<Order> newOrders = new ArrayList<>();
                        // transform the orders with the failed information
                        for (Order order : o.order()) {
                            Order transformed = (Order) order.transformUp(ua -> resolveMetadataToMessage(ua, failedAttrs, "order"),
                                    UnresolvedAttribute.class);
                            newOrders.add(order.equals(transformed) ? order : transformed);
                        }

                        return o.order().equals(newOrders) ? o : new OrderBy(o.location(), o.child(), newOrders);
                    }

                    // everything worked
                    return new Project(o.location(), new OrderBy(o.location(), newChild, maybeResolved), o.child().output());
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
                    List<Attribute> failedAttrs = new ArrayList<>();
                    LogicalPlan newChild = propagateMissing(f.child(), missing, failedAttrs);

                    // resolution failed and the failed expressions might contain resolution information so copy it over
                    if (!failedAttrs.isEmpty()) {
                        // transform the orders with the failed information
                        Expression transformed = f.condition().transformUp(ua -> resolveMetadataToMessage(ua, failedAttrs, "filter"),
                                UnresolvedAttribute.class);

                        return f.condition().equals(transformed) ? f : new Filter(f.location(), f.child(), transformed);
                    }

                    return new Project(f.location(), new Filter(f.location(), newChild, maybeResolved), f.child().output());
                }

                if (!maybeResolved.equals(f.condition())) {
                    return new Filter(f.location(), f.child(), maybeResolved);
                }
            }

            return plan;
        }

        static <E extends Expression> E tryResolveExpression(E exp, LogicalPlan plan) {
            E resolved = resolveExpression(exp, plan);
            if (!resolved.resolved()) {
                // look at unary trees but ignore subqueries
                if (plan.children().size() == 1 && !(plan instanceof SubQueryAlias)) {
                    return tryResolveExpression(resolved, plan.children().get(0));
                }
            }
            return resolved;
        }


        private static LogicalPlan propagateMissing(LogicalPlan plan, AttributeSet missing, List<Attribute> failed) {
            // no more attributes, bail out
            if (missing.isEmpty()) {
                return plan;
            }

            if (plan instanceof Project) {
                Project p = (Project) plan;
                AttributeSet diff = missing.substract(p.child().outputSet());
                return new Project(p.location(), propagateMissing(p.child(), diff, failed), combine(p.projections(), missing));
            }

            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                // missing attributes can only be grouping expressions
                for (Attribute m : missing) {
                    // but we don't can't add an agg if the group is missing
                    if (!Expressions.anyMatch(a.groupings(), m::semanticEquals)) {
                        if (m instanceof Attribute) {
                            // pass failure information to help the verifier
                            m = new UnresolvedAttribute(m.location(), m.name(), m.qualifier(), null, null,
                                    new AggGroupingFailure(Expressions.names(a.groupings())));
                        }
                        failed.add(m);
                    }
                }
                // propagation failed, return original plan
                if (!failed.isEmpty()) {
                    return plan;
                }
                return new Aggregate(a.location(), a.child(), a.groupings(), combine(a.aggregates(), missing));
            }

            // LeafPlans are tables and BinaryPlans are joins so pushing can only happen on unary
            if (plan instanceof UnaryPlan) {
                return plan.replaceChildren(singletonList(propagateMissing(((UnaryPlan) plan).child(), missing, failed)));
            }

            failed.addAll(missing);
            return plan;
        }

        private static UnresolvedAttribute resolveMetadataToMessage(UnresolvedAttribute ua, List<Attribute> attrs, String actionName) {
            for (Attribute attr : attrs) {
                if (ua.resolutionMetadata() == null && attr.name().equals(ua.name())) {
                    if (attr instanceof UnresolvedAttribute) {
                        UnresolvedAttribute fua = (UnresolvedAttribute) attr;
                        Object metadata = fua.resolutionMetadata();
                        if (metadata instanceof AggGroupingFailure) {
                            List<String> names = ((AggGroupingFailure) metadata).expectedGrouping;
                            return ua.withUnresolvedMessage(
                                    "Cannot " + actionName + " by non-grouped column [" + ua.qualifiedName() + "], expected " + names);
                        }
                    }
                }
            }
            return ua;
        };
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

                    if (uf.analyzed()) {
                        return uf;
                    }

                    String name = uf.name();

                    if (hasStar(uf.arguments())) {
                        uf = uf.preprocessStar();
                        if (uf.analyzed()) {
                            return uf;
                        }
                    }

                    if (!uf.childrenResolved()) {
                        return uf;
                    }

                    String functionName = functionRegistry.resolveAlias(name);

                    List<Function> list = getList(seen, functionName);
                    // first try to resolve from seen functions
                    if (!list.isEmpty()) {
                        for (Function seenFunction : list) {
                            if (uf.arguments().equals(seenFunction.arguments())) {
                                return seenFunction;
                            }
                        }
                    }

                    // not seen before, use the registry
                    if (!functionRegistry.functionExists(functionName)) {
                        return uf.missing(functionName, functionRegistry.listFunctions());
                    }
                    // TODO: look into Generator for significant terms, etc..
                    FunctionDefinition def = functionRegistry.resolveFunction(functionName);
                    Function f = uf.buildResolved(configuration, def);

                    list.add(f);
                    return f;
                }
                return e;
            });
        }

        private List<Function> getList(Map<String, List<Function>> seen, String name) {
            List<Function> list = seen.get(name);
            if (list == null) {
                list = new ArrayList<>();
                seen.put(name, list);
            }
            return list;
        }
    }

    private static class ResolveAliases extends AnalyzeRule<LogicalPlan> {

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
            return expressions != null && expressions.stream().anyMatch(e -> e instanceof UnresolvedAlias);
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
                        if (c.field() instanceof NamedExpression) {
                            return new Alias(c.location(), ((NamedExpression) c.field()).name(), c);
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
    private static class ProjectedAggregations extends AnalyzeRule<Project> {

        @Override
        protected LogicalPlan rule(Project p) {
            if (containsAggregate(p.projections())) {
                return new Aggregate(p.location(), p.child(), emptyList(), p.projections());
            }
            return p;
        }
    };

    //
    // Handle aggs in HAVING. To help folding any aggs not found in Aggregation
    // will be pushed down to the Aggregate and then projected. This also simplifies the Verifier's job.
    //
    private class ResolveAggsInHaving extends AnalyzeRule<LogicalPlan> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            // HAVING = Filter followed by an Agg
            if (plan instanceof Filter) {
                Filter f = (Filter) plan;
                if (f.child() instanceof Aggregate && f.child().resolved()) {
                    Aggregate agg = (Aggregate) f.child();

                    Set<NamedExpression> missing = null;
                    Expression condition = f.condition();

                    // the condition might contain an agg (AVG(salary)) that could have been resolved
                    // (salary cannot be pushed down to Aggregate since there's no grouping and thus the function wasn't resolved either)

                    // so try resolving the condition in one go through a 'dummy' aggregate
                    if (!condition.resolved()) {
                        // that's why try to resolve the condition
                        Aggregate tryResolvingCondition = new Aggregate(agg.location(), agg.child(), agg.groupings(),
                                singletonList(new Alias(f.location(), ".having", condition)));

                        LogicalPlan conditionResolved = analyze(tryResolvingCondition, false);

                        // if it got resolved
                        if (conditionResolved.resolved()) {
                            // replace the condition with the resolved one
                            condition = ((Alias) ((Aggregate) conditionResolved).aggregates().get(0)).child();
                        } else {
                            // else bail out
                            return plan;
                        }
                    }

                    missing = findMissingAggregate(agg, condition);

                    if (!missing.isEmpty()) {
                        Aggregate newAgg = new Aggregate(agg.location(), agg.child(), agg.groupings(),
                                combine(agg.aggregates(), missing));
                        Filter newFilter = new Filter(f.location(), newAgg, condition);
                        // preserve old output
                        return new Project(f.location(), newFilter, f.output());
                    }
                }
                return plan;
            }

            return plan;
        }

        private Set<NamedExpression> findMissingAggregate(Aggregate target, Expression from) {
            Set<NamedExpression> missing = new LinkedHashSet<>();

            for (Expression filterAgg : from.collect(Functions::isAggregate)) {
                if (!Expressions.anyMatch(target.aggregates(),
                        a -> {
                            Attribute attr = Expressions.attribute(a);
                            return attr != null && attr.semanticEquals(Expressions.attribute(filterAgg));
                        })) {
                    missing.add(Expressions.wrapAsNamed(filterAgg));
                }
            }

            return missing;
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

    private class ImplicitCasting extends AnalyzeRule<LogicalPlan> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan.transformExpressionsDown(this::implicitCast);
        }

        private Expression implicitCast(Expression e) {
            if (!e.childrenResolved()) {
                return e;
            }

            Expression left = null, right = null;

            // BinaryOperations are ignored as they are pushed down to ES
            // and casting (and thus Aliasing when folding) gets in the way

            if (e instanceof ArithmeticOperation) {
                ArithmeticOperation f = (ArithmeticOperation) e;
                left = f.left();
                right = f.right();
            }

            if (left != null) {
                DataType l = left.dataType();
                DataType r = right.dataType();
                if (l != r) {
                    DataType common = DataTypeConversion.commonType(l, r);
                    if (common == null) {
                        return e;
                    }
                    left = l == common ? left : new Cast(left.location(), left, common);
                    right = r == common ? right : new Cast(right.location(), right, common);
                    return e.replaceChildren(Arrays.asList(left, right));
                }
            }

            return e;
        }
    }

    abstract static class AnalyzeRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t), typeToken());
        }

        @Override
        protected abstract LogicalPlan rule(SubPlan plan);

        protected boolean skipResolved() {
            return true;
        }
    }
}
