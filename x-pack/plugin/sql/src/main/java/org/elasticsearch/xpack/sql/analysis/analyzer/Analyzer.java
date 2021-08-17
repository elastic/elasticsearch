/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.analysis.analyzer;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.AddMissingEqualsToBoolField;
import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeMap;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAlias;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedStar;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.ql.expression.function.Functions;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.OrderBy;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.InvalidMappedField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.ql.util.CollectionUtils;
import org.elasticsearch.xpack.ql.util.Holder;
import org.elasticsearch.xpack.sql.expression.SubQueryExpression;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionResolution;
import org.elasticsearch.xpack.sql.expression.function.scalar.Cast;
import org.elasticsearch.xpack.sql.plan.logical.Join;
import org.elasticsearch.xpack.sql.plan.logical.LocalRelation;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;
import org.elasticsearch.xpack.sql.plan.logical.SubQueryAlias;
import org.elasticsearch.xpack.sql.plan.logical.With;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.AnalyzerRule;
import static org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.BaseAnalyzerRule;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.sql.session.VersionCompatibilityChecks.isTypeSupportedInVersion;

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
    private final SqlConfiguration configuration;
    /**
     * The verifier has the role of checking the analyzed tree for failures and build a list of failures.
     */
    private final Verifier verifier;

    public Analyzer(SqlConfiguration configuration, FunctionRegistry functionRegistry, IndexResolution results, Verifier verifier) {
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
                new ResolveFilterRefs(),
                new ResolveFunctions(),
                new ResolveAliases(),
                new ProjectedAggregations(),
                new HavingOverProject(),
                new ResolveAggsInHaving(),
                new ResolveAggsInOrderBy()
                //new ImplicitCasting()
                );
        Batch finish = new Batch("Finish Analysis",
                new ReplaceSubQueryAliases(), // Should be run before pruning SubqueryAliases
                new PruneSubQueryAliases(),
                new AddMissingEqualsToBoolField(),
                CleanAliases.INSTANCE
                );
        return Arrays.asList(substitution, resolution, finish);
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
        if (failures.isEmpty() == false) {
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
            if (attribute.synthetic() == false) {
                boolean match = qualified ?
                        Objects.equals(u.qualifiedName(), attribute.qualifiedName()) :
                        // if the field is unqualified
                        // first check the names directly
                        (Objects.equals(u.name(), attribute.name())
                             // but also if the qualifier might not be quoted and if there's any ambiguity with nested fields
                             || Objects.equals(u.name(), attribute.qualifiedName()));
                if (match) {
                    matches.add(attribute);
                }
            }
        }

        // none found
        if (matches.isEmpty()) {
            return null;
        }

        if (matches.size() == 1) {
            // only add the location if the match is univocal; b/c otherwise adding the location will overwrite any preexisting one
            return handleSpecialFields(u, matches.get(0).withLocation(u.source()), allowCompound);
        }

        List<String> refs = matches.stream()
            .sorted((a, b) -> {
                int lineDiff = a.sourceLocation().getLineNumber() - b.sourceLocation().getLineNumber();
                int colDiff = a.sourceLocation().getColumnNumber() - b.sourceLocation().getColumnNumber();
                return lineDiff != 0 ? lineDiff : (colDiff != 0 ? colDiff : a.qualifiedName().compareTo(b.qualifiedName()));
            })
            .map(a -> "line " + a.sourceLocation().toString().substring(1) + " [" +
                (a.qualifier() != null ? "\"" + a.qualifier() + "\".\"" + a.name() + "\"" : a.name()) + "]")
            .collect(toList());
        return u.withUnresolvedMessage("Reference [" + u.qualifiedName() + "] is ambiguous (to disambiguate use quotes or qualifiers); " +
            "matches any of " + refs);
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
                if (unsupportedField.hasInherited()) {
                    named = u.withUnresolvedMessage(
                            "Cannot use field [" + fa.name() + "] with unsupported type [" + unsupportedField.getOriginalType() + "] "
                                    + "in hierarchy (field [" + unsupportedField.getInherited() + "])");
                } else {
                    named = u.withUnresolvedMessage(
                            "Cannot use field [" + fa.name() + "] with unsupported type [" + unsupportedField.getOriginalType() + "]");
                }
            }
            // compound fields
            else if (allowCompound == false && DataTypes.isPrimitive(fa.dataType()) == false) {
                named = u.withUnresolvedMessage(
                        "Cannot use field [" + fa.name() + "] type [" + fa.dataType().typeName() + "] only its subfields");
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

    private static class CTESubstitution extends AnalyzerRule<With> {

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
                        return new SubQueryAlias(ur.source(), subQueryAlias, ur.alias());
                    }
                    return subQueryAlias;
                }
                return ur;
            }
            // inlined queries (SELECT 1 + 2) are already resolved
            else if (p instanceof LocalRelation) {
                return p;
            }

            return p.transformExpressionsDown(SubQueryExpression.class, sq -> sq.withQuery(substituteCTE(sq.query(), subQueries)));
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }

    private class ResolveTable extends AnalyzerRule<UnresolvedRelation> {
        @Override
        protected LogicalPlan rule(UnresolvedRelation plan) {
            TableIdentifier table = plan.table();
            if (indexResolution.isValid() == false) {
                return plan.unresolvedMessage().equals(indexResolution.toString()) ? plan :
                    new UnresolvedRelation(plan.source(), plan.table(), plan.alias(), plan.frozen(), indexResolution.toString());
            }
            assert indexResolution.matches(table.index());
            LogicalPlan logicalPlan = new EsRelation(plan.source(), indexResolution.get(), plan.frozen());
            SubQueryAlias sa = new SubQueryAlias(plan.source(), logicalPlan, table.index());

            if (plan.alias() != null) {
                sa = new SubQueryAlias(plan.source(), sa, plan.alias());
            }

            return sa;
        }
    }

    private class ResolveRefs extends BaseAnalyzerRule {

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            if (plan instanceof Project) {
                Project p = (Project) plan;
                if (hasStar(p.projections())) {
                    return new Project(p.source(), p.child(), expandProjections(p.projections(), p.child()));
                }
            }
            else if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                if (hasStar(a.aggregates())) {
                    return new Aggregate(a.source(), a.child(), a.groupings(),
                            expandProjections(a.aggregates(), a.child()));
                }
                // if the grouping is unresolved but the aggs are, use the latter to resolve the former.
                // solves the case of queries declaring an alias in SELECT and referring to it in GROUP BY.
                // e.g. SELECT x AS a ... GROUP BY a
                if (a.expressionsResolved() == false && Resolvables.resolved(a.aggregates())) {
                    List<Expression> groupings = a.groupings();
                    List<Expression> newGroupings = new ArrayList<>();
                    List<Tuple<Attribute, Expression>> resolvedAliases = Expressions.aliases(a.aggregates());

                    boolean changed = false;
                    for (Expression grouping : groupings) {
                        if (grouping instanceof UnresolvedAttribute) {
                            Attribute maybeResolved = resolveAgainstList((UnresolvedAttribute) grouping,
                                resolvedAliases.stream().map(Tuple::v1).collect(toList()));
                            if (maybeResolved != null) {
                                changed = true;
                                if (maybeResolved.resolved()) {
                                    grouping = resolvedAliases.stream()
                                        .filter(t -> t.v1().equals(maybeResolved))
                                        // use the matched expression (not its attribute)
                                        .map(Tuple::v2)
                                        .findAny()
                                        .get(); // there should always be exactly one match
                                } else {
                                    grouping = maybeResolved;
                                }
                            }
                        }
                        newGroupings.add(grouping);
                    }

                    return changed ? new Aggregate(a.source(), a.child(), newGroupings, a.aggregates()) : a;
                }
            }

            else if (plan instanceof Join) {
                Join j = (Join) plan;
                if (j.duplicatesResolved() == false) {
                    LogicalPlan deduped = dedupRight(j.left(), j.right());
                    return new Join(j.source(), j.left(), deduped, j.type(), j.condition());
                }
            }
            // try resolving the order expression (the children are resolved as this point)
            else if (plan instanceof OrderBy) {
                OrderBy o = (OrderBy) plan;
                if (o.resolved() == false) {
                    List<Order> resolvedOrder = new ArrayList<>(o.order().size());
                    for (Order order : o.order()) {
                        resolvedOrder.add(resolveExpression(order, o.child()));
                    }
                    return new OrderBy(o.source(), o.child(), resolvedOrder);
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Attempting to resolve {}", plan.nodeString());
            }

            return plan.transformExpressionsUp(UnresolvedAttribute.class, u -> {
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
                //TODO: likely have to expand * inside functions as well
                return u;
            });
        }

        private List<NamedExpression> expandProjections(List<? extends NamedExpression> projections, LogicalPlan child) {
            List<NamedExpression> result = new ArrayList<>();

            List<Attribute> output = child.output();
            for (NamedExpression ne : projections) {
                if (ne instanceof UnresolvedStar) {
                    List<NamedExpression> expanded = expandStar((UnresolvedStar) ne, output);

                    // the field exists, but cannot be expanded (no sub-fields)
                    result.addAll(expanded);
                } else if (ne instanceof UnresolvedAlias) {
                    UnresolvedAlias ua = (UnresolvedAlias) ne;
                    if (ua.child() instanceof UnresolvedStar) {
                        result.addAll(expandStar((UnresolvedStar) ua.child(), output));
                    }
                } else {
                    result.add(ne);
                }
            }

            return filterUnsupportedProjections(result);
        }

        private List<NamedExpression> filterUnsupportedProjections(List<NamedExpression> projections) {
            return projections.stream()
                .filter(p -> p.resolved() == false || isTypeSupportedInVersion(p.dataType(), configuration.version()))
                .collect(toList());
        }

        private List<NamedExpression> expandStar(UnresolvedStar us, List<Attribute> output) {
            List<NamedExpression> expanded = new ArrayList<>();

            // a qualifier is specified - since this is a star, it should be a CompoundDataType
            if (us.qualifier() != null) {
                // resolve the so-called qualifier first
                // since this is an unresolved star we don't know whether it's a path or an actual qualifier
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
                // qualifier resolves to a non-struct field and cannot be expanded
                else if (DataTypes.isPrimitive(q.dataType())) {
                    return singletonList(us);
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
                                expanded.add(fa.withLocation(attr.source()));
                            }
                        } else {
                            // use the path only to match non-compound types
                            if (Objects.equals(q.name(), fa.path())) {
                                expanded.add(fa.withLocation(attr.source()));
                            }
                        }
                    }
                }
            } else {
                expanded.addAll(Expressions.onlyPrimitiveFieldAttributes(output));
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
    private static class ResolveOrdinalInOrderByAndGroupBy extends BaseAnalyzerRule {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            if (plan instanceof OrderBy) {
                OrderBy orderBy = (OrderBy) plan;
                boolean changed = false;

                List<Order> newOrder = new ArrayList<>(orderBy.order().size());
                List<Attribute> ordinalReference = orderBy.child().output();
                int max = ordinalReference.size();

                for (Order order : orderBy.order()) {
                    Expression child = order.child();
                    Integer ordinal = findOrdinal(order.child());
                    if (ordinal != null) {
                        changed = true;
                        if (ordinal > 0 && ordinal <= max) {
                            newOrder.add(new Order(order.source(), orderBy.child().output().get(ordinal - 1), order.direction(),
                                    order.nullsPosition()));
                        }
                        else {
                            // report error
                            String message = LoggerMessageFormat.format("Invalid ordinal [{}] specified in [{}] (valid range is [1, {}])",
                                    ordinal, orderBy.sourceText(), max);
                            UnresolvedAttribute ua = new UnresolvedAttribute(child.source(), orderBy.sourceText(), null, message);
                            newOrder.add(new Order(order.source(), ua, order.direction(), order.nullsPosition()));
                        }
                    }
                    else {
                        newOrder.add(order);
                    }
                }

                return changed ? new OrderBy(orderBy.source(), orderBy.child(), newOrder) : orderBy;
            }

            if (plan instanceof Aggregate) {
                Aggregate agg = (Aggregate) plan;

                if (Resolvables.resolved(agg.aggregates()) == false) {
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
                        String errorMessage = null;
                        if (ordinal > 0 && ordinal <= max) {
                            NamedExpression reference = aggregates.get(ordinal - 1);
                            if (containsAggregate(reference)) {
                                errorMessage = LoggerMessageFormat.format(
                                        "Ordinal [{}] in [{}] refers to an invalid argument, aggregate function [{}]",
                                        ordinal, agg.sourceText(), reference.sourceText());

                            } else {
                                newGroupings.add(reference);
                            }
                        }
                        else {
                            errorMessage = LoggerMessageFormat.format("Invalid ordinal [{}] specified in [{}] (valid range is [1, {}])",
                                    ordinal, agg.sourceText(), max);
                        }
                        if (errorMessage != null) {
                            newGroupings.add(new UnresolvedAttribute(exp.source(), agg.sourceText(), null, errorMessage));
                        }
                    }
                    else {
                        newGroupings.add(exp);
                    }
                }

                return changed ? new Aggregate(agg.source(), agg.child(), newGroupings, aggregates) : agg;
            }

            return plan;
        }

        private Integer findOrdinal(Expression expression) {
            if (expression.foldable()) {
                if (expression.dataType().isInteger()) {
                    Object v = Foldables.valueOf(expression);
                    if (v instanceof Number) {
                        return ((Number) v).intValue();
                    }
                }
            }
            return null;
        }
    }

    // It is valid to filter (including HAVING) or sort by attributes not present in the SELECT clause.
    // This rule pushed down the attributes for them to be resolved then projects them away.
    // As such this rule is an extended version of ResolveRefs
    private static class ResolveMissingRefs extends BaseAnalyzerRule {

        private static class AggGroupingFailure {
            final List<String> expectedGrouping;

            private AggGroupingFailure(List<String> expectedGrouping) {
                this.expectedGrouping = expectedGrouping;
            }
        }

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            if (plan instanceof OrderBy) {
                OrderBy o = (OrderBy) plan;
                LogicalPlan child = o.child();
                List<Order> maybeResolved = new ArrayList<>();
                for (Order or : o.order()) {
                    maybeResolved.add(or.resolved() ? or : tryResolveExpression(or, child));
                }

                Stream<Order> referencesStream = maybeResolved.stream()
                        .filter(Expression::resolved);

                // if there are any references in the output
                // try and resolve them to the source in order to compare the source expressions
                // e.g. ORDER BY a + 1
                //      \ SELECT a + 1
                // a + 1 in SELECT is actually Alias("a + 1", a + 1) and translates to ReferenceAttribute
                // in the output. However it won't match the unnamed a + 1 despite being the same expression
                // so explicitly compare the source

                // if there's a match, remove the item from the reference stream
                if (Expressions.hasReferenceAttribute(child.outputSet())) {
                    AttributeMap.Builder<Expression> builder = AttributeMap.builder();
                    // collect aliases
                    child.forEachUp(p -> p.forEachExpressionUp(Alias.class, a -> builder.put(a.toAttribute(), a.child())));
                    final AttributeMap<Expression> collectRefs = builder.build();

                    referencesStream = referencesStream.filter(r -> {
                        for (Attribute attr : child.outputSet()) {
                            if (attr instanceof ReferenceAttribute) {
                                Expression source = collectRefs.resolve(attr, attr);
                                // found a match, no need to resolve it further
                                // so filter it out
                                if (source.equals(r.child())) {
                                    return false;
                                }
                            }
                        }
                        return true;
                    });
                }

                AttributeSet resolvedRefs = Expressions.references(referencesStream.collect(toList()));

                AttributeSet missing = resolvedRefs.subtract(child.outputSet());

                if (missing.isEmpty() == false) {
                    // Add missing attributes but project them away afterwards
                    List<Attribute> failedAttrs = new ArrayList<>();
                    LogicalPlan newChild = propagateMissing(o.child(), missing, failedAttrs);

                    // resolution failed and the failed expressions might contain resolution information so copy it over
                    if (failedAttrs.isEmpty() == false) {
                        List<Order> newOrders = new ArrayList<>();
                        // transform the orders with the failed information
                        for (Order order : o.order()) {
                            Order transformed = (Order) order.transformUp(UnresolvedAttribute.class,
                                ua -> resolveMetadataToMessage(ua, failedAttrs, "order")
                            );
                            newOrders.add(order.equals(transformed) ? order : transformed);
                        }

                        return o.order().equals(newOrders) ? o : new OrderBy(o.source(), o.child(), newOrders);
                    }

                    // everything worked
                    return new Project(o.source(), new OrderBy(o.source(), newChild, maybeResolved), o.child().output());
                }

                if (maybeResolved.equals(o.order()) == false) {
                    return new OrderBy(o.source(), o.child(), maybeResolved);
                }
            }

            if (plan instanceof Filter) {
                Filter f = (Filter) plan;
                Expression maybeResolved = tryResolveExpression(f.condition(), f.child());

                AttributeSet resolvedRefs = new AttributeSet(maybeResolved.references().stream()
                        .filter(Expression::resolved)
                        .collect(toList()));

                AttributeSet missing = resolvedRefs.subtract(f.child().outputSet());

                if (missing.isEmpty() == false) {
                    // Again, add missing attributes and project them away
                    List<Attribute> failedAttrs = new ArrayList<>();
                    LogicalPlan newChild = propagateMissing(f.child(), missing, failedAttrs);

                    // resolution failed and the failed expressions might contain resolution information so copy it over
                    if (failedAttrs.isEmpty() == false) {
                        // transform the orders with the failed information
                        Expression transformed = f.condition().transformUp(UnresolvedAttribute.class,
                            ua -> resolveMetadataToMessage(ua, failedAttrs, "filter")
                        );

                        return f.condition().equals(transformed) ? f : f.with(transformed);
                    }

                    return new Project(f.source(), f.with(newChild, maybeResolved), f.child().output());
                }

                if (maybeResolved.equals(f.condition()) == false) {
                    return f.with(maybeResolved);
                }
            }

            // Try to resolve aggregates and groupings based on the child plan
            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                LogicalPlan child = a.child();
                List<Expression> newGroupings = new ArrayList<>(a.groupings().size());
                a.groupings().forEach(e -> newGroupings.add(tryResolveExpression(e, child)));
                List<NamedExpression> newAggregates = new ArrayList<>(a.aggregates().size());
                a.aggregates().forEach(e -> newAggregates.add(tryResolveExpression(e, child)));
                if (newAggregates.equals(a.aggregates()) == false || newGroupings.equals(a.groupings()) == false) {
                    return new Aggregate(a.source(), child, newGroupings, newAggregates);
                }
            }
            return plan;
        }

        static <E extends Expression> E tryResolveExpression(E exp, LogicalPlan plan) {
            E resolved = resolveExpression(exp, plan);
            if (resolved.resolved() == false) {
                // look at unary trees but ignore subqueries
                if (plan.children().size() == 1 && (plan instanceof SubQueryAlias) == false) {
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
                AttributeSet diff = missing.subtract(p.child().outputSet());
                return new Project(p.source(), propagateMissing(p.child(), diff, failed), combine(p.projections(), missing));
            }

            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                // missing attributes can only be grouping expressions
                // however take into account aliased groups
                // SELECT x AS i ... GROUP BY i
                for (Attribute m : missing) {
                    // but we can't add an agg if the group is missing
                    if (Expressions.match(a.groupings(), m::semanticEquals) == false) {
                        // pass failure information to help the verifier
                        m = new UnresolvedAttribute(m.source(), m.name(), m.qualifier(), null, null,
                                new AggGroupingFailure(Expressions.names(a.groupings())));
                        failed.add(m);
                    }
                }
                // propagation failed, return original plan
                if (failed.isEmpty() == false) {
                    return plan;
                }
                return new Aggregate(a.source(), a.child(), a.groupings(), combine(a.aggregates(), missing));
            }

            // LeafPlans are tables and BinaryPlans are joins so pushing can only happen on unary
            if (plan instanceof UnaryPlan) {
                UnaryPlan unary = (UnaryPlan) plan;
                return unary.replaceChild(propagateMissing(unary.child(), missing, failed));
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
        }
    }

    //
    // Resolve aliases defined in SELECT that are referred inside the WHERE clause:
    // SELECT int AS i FROM t WHERE i > 10
    //
    // As such, identify all project and aggregates that have a Filter child
    // and look at any resolved aliases that match and replace them.
    private static class ResolveFilterRefs extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan instanceof Project) {
                Project p = (Project) plan;
                if (p.child() instanceof Filter) {
                    Filter f = (Filter) p.child();
                    Expression condition = f.condition();
                    if (condition.resolved() == false && f.childrenResolved()) {
                        Expression newCondition = replaceAliases(condition, p.projections());
                        if (newCondition != condition) {
                            return new Project(p.source(), f.with(newCondition), p.projections());
                        }
                    }
                }
            }

            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                if (a.child() instanceof Filter) {
                    Filter f = (Filter) a.child();
                    Expression condition = f.condition();
                    if (condition.resolved() == false && f.childrenResolved()) {
                        Expression newCondition = replaceAliases(condition, a.aggregates());
                        if (newCondition != condition) {
                            return new Aggregate(a.source(), f.with(newCondition), a.groupings(), a.aggregates());
                        }
                    }
                }
            }

            return plan;
        }

        private Expression replaceAliases(Expression condition, List<? extends NamedExpression> named) {
            List<Alias> aliases = new ArrayList<>();
            named.forEach(n -> {
                if (n instanceof Alias) {
                    aliases.add((Alias) n);
                }
            });

            return condition.transformDown(UnresolvedAttribute.class, u -> {
                boolean qualified = u.qualifier() != null;
                for (Alias alias : aliases) {
                    // don't replace field with their own aliases (it creates infinite cycles)
                    if (alias.anyMatch(e -> e == u) == false &&
                        (qualified ?
                            Objects.equals(alias.qualifiedName(), u.qualifiedName()) :
                            Objects.equals(alias.name(), u.name()))) {
                        return alias;
                    }
                }
                return u;
            });
        }
    }

    private class ResolveFunctions extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan.transformExpressionsUp(UnresolvedFunction.class, uf -> {
                if (uf.analyzed()) {
                    return uf;
                }

                String name = uf.name();

                if (hasStar(uf.arguments())) {
                    FunctionResolutionStrategy strategy = uf.resolutionStrategy();
                    if (SqlFunctionResolution.DISTINCT == strategy) {
                        uf = uf.withMessage("* is not valid with DISTINCT");
                    } else if (SqlFunctionResolution.EXTRACT == strategy) {
                        uf = uf.withMessage("Can't extract from *");
                    } else {
                        if (uf.name().toUpperCase(Locale.ROOT).equals("COUNT")) {
                            uf = new UnresolvedFunction(uf.source(), uf.name(), strategy,
                                singletonList(new Literal(uf.arguments().get(0).source(), Integer.valueOf(1), DataTypes.INTEGER)));
                        }
                    }
                    if (uf.analyzed()) {
                        return uf;
                    }
                }

                if (uf.childrenResolved() == false) {
                    return uf;
                }

                String functionName = functionRegistry.resolveAlias(name);
                if (functionRegistry.functionExists(functionName) == false) {
                    return uf.missing(functionName, functionRegistry.listFunctions());
                }
                // TODO: look into Generator for significant terms, etc..
                FunctionDefinition def = functionRegistry.resolveFunction(functionName);
                Function f = uf.buildResolved(configuration, def);
                return f;
            });
        }
    }

    private static class ResolveAliases extends BaseAnalyzerRule {

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            if (plan instanceof Project) {
                Project p = (Project) plan;
                if (hasUnresolvedAliases(p.projections())) {
                    return new Project(p.source(), p.child(), assignAliases(p.projections()));
                }
                return p;
            }
            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                if (hasUnresolvedAliases(a.aggregates())) {
                    return new Aggregate(a.source(), a.child(), a.groupings(), assignAliases(a.aggregates()));
                }
                return a;
            }
            if (plan instanceof Pivot) {
                Pivot p = (Pivot) plan;
                if (hasUnresolvedAliases(p.values())) {
                    p = new Pivot(p.source(), p.child(), p.column(), assignAliases(p.values()), p.aggregates());
                }
                if (hasUnresolvedAliases(p.aggregates())) {
                    p = new Pivot(p.source(), p.child(), p.column(), p.values(), assignAliases(p.aggregates()));
                }
                return p;
            }

            return plan;
        }

        private boolean hasUnresolvedAliases(List<? extends NamedExpression> expressions) {
            return expressions != null && Expressions.anyMatch(expressions, UnresolvedAlias.class::isInstance);
        }

        private List<NamedExpression> assignAliases(List<? extends NamedExpression> exprs) {
            List<NamedExpression> newExpr = new ArrayList<>(exprs.size());
            for (NamedExpression expr : exprs) {
                NamedExpression transformed = (NamedExpression) expr.transformUp(UnresolvedAlias.class, ua -> {
                    Expression child = ua.child();
                    if (child instanceof NamedExpression) {
                        return child;
                    }
                    if (child.resolved() == false) {
                        return ua;
                    }
                    if (child instanceof Cast) {
                        Cast c = (Cast) child;
                        if (c.field() instanceof NamedExpression) {
                            return new Alias(c.source(), ((NamedExpression) c.field()).name(), c);
                        }
                    }
                    return new Alias(child.source(), child.sourceText(), child);
                });
                newExpr.add(expr.equals(transformed) ? expr : transformed);
            }
            return newExpr;
        }
    }


    //
    // Replace a project with aggregation into an aggregation
    //
    private static class ProjectedAggregations extends AnalyzerRule<Project> {

        @Override
        protected LogicalPlan rule(Project p) {
            if (containsAggregate(p.projections())) {
                return new Aggregate(p.source(), p.child(), emptyList(), p.projections());
            }
            return p;
        }
    }

    //
    // Detect implicit grouping with filtering and convert them into aggregates.
    // SELECT 1 FROM x HAVING COUNT(*) > 0
    // is a filter followed by projection and fails as the engine does not
    // understand it is an implicit grouping.
    //
    private static class HavingOverProject extends AnalyzerRule<Filter> {

        @Override
        protected LogicalPlan rule(Filter f) {
            if (f.child() instanceof Project) {
                Project p = (Project) f.child();

                for (Expression n : p.projections()) {
                    if (n instanceof Alias) {
                        n = ((Alias) n).child();
                    }
                    // no literal or aggregates - it's a 'regular' projection
                    if (n.foldable() == false && Functions.isAggregate(n) == false
                            // folding might not work (it might wait for the optimizer)
                            // so check whether any column is referenced
                            && n.anyMatch(FieldAttribute.class::isInstance)) {
                        return f;
                    }
                }

                if (containsAggregate(f.condition())) {
                    return f.with(new Aggregate(p.source(), p.child(), emptyList(), p.projections()), f.condition());
                }
            }
            return f;
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }

    //
    // Handle aggs in HAVING. To help folding any aggs not found in Aggregation
    // will be pushed down to the Aggregate and then projected. This also simplifies the Verifier's job.
    //
    private class ResolveAggsInHaving extends AnalyzerRule<Filter> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(Filter f) {
            // HAVING = Filter followed by an Agg
            if (f.child() instanceof Aggregate && f.child().resolved()) {
                Aggregate agg = (Aggregate) f.child();

                Set<NamedExpression> missing = null;
                Expression condition = f.condition();

                // the condition might contain an agg (AVG(salary)) that could have been resolved
                // (salary cannot be pushed down to Aggregate since there's no grouping and thus the function wasn't resolved either)

                // so try resolving the condition in one go through a 'dummy' aggregate
                if (condition.resolved() == false) {
                    // that's why try to resolve the condition
                    Aggregate tryResolvingCondition = new Aggregate(agg.source(), agg.child(), agg.groupings(),
                            combine(agg.aggregates(), new Alias(f.source(), ".having", condition)));

                    tryResolvingCondition = (Aggregate) analyze(tryResolvingCondition, false);

                    // if it got resolved
                    if (tryResolvingCondition.resolved()) {
                        // replace the condition with the resolved one
                        condition = ((Alias) tryResolvingCondition.aggregates()
                            .get(tryResolvingCondition.aggregates().size() - 1)).child();
                    } else {
                        // else bail out
                        return f;
                    }
                }

                missing = findMissingAggregate(agg, condition);

                if (missing.isEmpty() == false) {
                    Aggregate newAgg = new Aggregate(agg.source(), agg.child(), agg.groupings(), combine(agg.aggregates(), missing));
                    Filter newFilter = f.with(newAgg, condition);
                    // preserve old output
                    return new Project(f.source(), newFilter, f.output());
                }

                return f.with(condition);
            }
            return f;
        }

        private Set<NamedExpression> findMissingAggregate(Aggregate target, Expression from) {
            Set<NamedExpression> missing = new LinkedHashSet<>();

            for (Expression filterAgg : from.collect(Functions::isAggregate)) {
                if (Expressions.anyMatch(target.aggregates(), a -> {
                    if (a instanceof Alias) {
                        a = ((Alias) a).child();
                    }
                    return a.equals(filterAgg);
                }) == false) {
                    missing.add(Expressions.wrapAsNamed(filterAgg));
                }
            }

            return missing;
        }
    }


    //
    // Handle aggs in ORDER BY. To help folding any aggs not found in Aggregation
    // will be pushed down to the Aggregate and then projected. This also simplifies the Verifier's job.
    // Similar to Having however using a different matching pattern since HAVING is always Filter with Agg,
    // while an OrderBy can have multiple intermediate nodes (Filter,Project, etc...)
    //
    private static class ResolveAggsInOrderBy extends AnalyzerRule<OrderBy> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(OrderBy ob) {
            List<Order> orders = ob.order();

            // 1. collect aggs inside an order by
            List<Expression> aggs = new ArrayList<>();
            for (Order order : orders) {
                if (Functions.isAggregate(order.child())) {
                    aggs.add(order.child());
                }
            }
            if (aggs.isEmpty()) {
                return ob;
            }

            // 2. find first Aggregate child and update it
            final Holder<Boolean> found = new Holder<>(Boolean.FALSE);

            LogicalPlan plan = ob.transformDown(Aggregate.class, a -> {
                if (found.get() == Boolean.FALSE) {
                    found.set(Boolean.TRUE);

                    List<NamedExpression> missing = new ArrayList<>();

                    for (Expression orderedAgg : aggs) {
                        if (Expressions.anyMatch(a.aggregates(), e -> {
                            if (e instanceof Alias) {
                                e = ((Alias) e).child();
                            }
                            return e.equals(orderedAgg);
                        }) == false) {
                            missing.add(Expressions.wrapAsNamed(orderedAgg));
                        }
                    }
                    // agg already contains all aggs
                    if (missing.isEmpty() == false) {
                        // save aggregates
                        return new Aggregate(a.source(), a.child(), a.groupings(), CollectionUtils.combine(a.aggregates(), missing));
                    }
                }
                return a;
            });

            // if the plan was updated, project the initial aggregates
            if (plan != ob) {
                return new Project(ob.source(), plan, ob.output());
            }
            return ob;
        }
    }

    private static class ImplicitCasting extends AnalyzerRule<LogicalPlan> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan.transformExpressionsDown(this::implicitCast);
        }

        private Expression implicitCast(Expression e) {
            if (e.childrenResolved() == false) {
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
                    DataType common = SqlDataTypeConverter.commonType(l, r);
                    if (common == null) {
                        return e;
                    }
                    left = l == common ? left : new Cast(left.source(), left, common);
                    right = r == common ? right : new Cast(right.source(), right, common);
                    return e.replaceChildrenSameSize(Arrays.asList(left, right));
                }
            }

            return e;
        }
    }

    public static class ReplaceSubQueryAliases extends AnalyzerRule<UnaryPlan> {

        @Override
        protected LogicalPlan rule(UnaryPlan plan) {
            if (plan.child() instanceof SubQueryAlias) {
                SubQueryAlias a = (SubQueryAlias) plan.child();
                return plan.transformExpressionsDown(FieldAttribute.class, f -> {
                   if (f.qualifier() != null && f.qualifier().equals(a.alias())) {
                       // Find the underlying concrete relation (EsIndex) and its name as the new qualifier
                       List<LogicalPlan> children = a.collectFirstChildren(p -> p instanceof EsRelation);
                       if (children.isEmpty() == false) {
                           return f.withQualifier(((EsRelation) children.get(0)).index().name());
                       }
                   }
                   return f;
                });
            }
            return plan;
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }

    public static class PruneSubQueryAliases extends AnalyzerRule<SubQueryAlias> {

        @Override
        protected LogicalPlan rule(SubQueryAlias alias) {
            return alias.child();
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }

    public static class CleanAliases extends AnalyzerRule<LogicalPlan> {

        public static final CleanAliases INSTANCE = new CleanAliases();

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan instanceof Project) {
                Project p = (Project) plan;
                return new Project(p.source(), p.child(), cleanChildrenAliases(p.projections()));
            }

            if (plan instanceof Aggregate) {
                Aggregate a = (Aggregate) plan;
                // aliases inside GROUP BY are irrelevant so remove all of them
                // however aggregations are important (ultimately a projection)
                return new Aggregate(a.source(), a.child(), cleanAllAliases(a.groupings()), cleanChildrenAliases(a.aggregates()));
            }

            if (plan instanceof Pivot) {
                Pivot p = (Pivot) plan;
                return new Pivot(p.source(), p.child(), trimAliases(p.column()), cleanChildrenAliases(p.values()),
                        cleanChildrenAliases(p.aggregates()));
            }

            return plan.transformExpressionsOnly(Alias.class, a -> a.child());
        }

        private List<NamedExpression> cleanChildrenAliases(List<? extends NamedExpression> args) {
            List<NamedExpression> cleaned = new ArrayList<>(args.size());
            for (NamedExpression ne : args) {
                cleaned.add((NamedExpression) trimNonTopLevelAliases(ne));
            }
            return cleaned;
        }

        private List<Expression> cleanAllAliases(List<Expression> args) {
            List<Expression> cleaned = new ArrayList<>(args.size());
            for (Expression e : args) {
                cleaned.add(trimAliases(e));
            }
            return cleaned;
        }

        public static Expression trimNonTopLevelAliases(Expression e) {
            if (e instanceof Alias) {
                Alias a = (Alias) e;
                return new Alias(a.source(), a.name(), a.qualifier(), trimAliases(a.child()), a.id());
            }
            return trimAliases(e);
        }

        private static Expression trimAliases(Expression e) {
            return e.transformDown(Alias.class, Alias::child);
        }

        @Override
        protected boolean skipResolved() {
            return false;
        }
    }
}
