/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.PlanToXContent;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * There are two main types of plans, {@code LogicalPlan} and {@code PhysicalPlan}
 */
public abstract class QueryPlan<PlanType extends QueryPlan<PlanType>> extends Node<PlanType> {

    private AttributeSet lazyOutputSet;
    private AttributeSet lazyInputSet;
    private List<Expression> lazyExpressions;
    private AttributeSet lazyReferences;

    /**
     * Assigned in the constructor. Same-node rewrites copy it via {@link #copyIdentityFrom}; it is not
     * part of {@code equals} or {@code hashCode}. See {@link PlanId}.
     */
    private PlanId planId;
    /** Nodes this one was derived from. Empty until a translation such as logical-to-physical records them. */
    private List<PlanId> origin;

    public QueryPlan(Source source, List<PlanType> children) {
        super(source, children);
        this.planId = new PlanId();
        this.origin = List.of();
    }

    public final PlanId planId() {
        return planId;
    }

    /**
     * Ids this node was derived from, in the order they were recorded. Never null.
     * Named {@code planOrigin} because {@code TimeSeriesAggregate} already uses {@code origin()}
     * for a different enum. JSON still writes the field as {@code origin}.
     */
    public final List<PlanId> planOrigin() {
        return origin;
    }

    /**
     * Copies {@link #planId} and {@link #origin} from {@code original} onto this node.
     * {@code replaceChildren} does this so a rewrite of the same node keeps its identity.
     * The fields are mutable on purpose, the same way {@code LogicalPlan} stage is set after construction.
     */
    @SuppressWarnings("unchecked")
    public final PlanType copyIdentityFrom(QueryPlan<?> original) {
        if (original != null && original != this) {
            this.planId = original.planId;
            this.origin = original.origin;
        }
        return (PlanType) this;
    }

    /**
     * Appends {@code ids} to {@link #origin}. Existing entries are kept.
     * A 1:1 logical-to-physical translation uses this to record the logical node it replaces.
     */
    @SuppressWarnings("unchecked")
    public final PlanType withOrigin(PlanId... ids) {
        if (ids == null || ids.length == 0) {
            return (PlanType) this;
        }
        List<PlanId> merged = new ArrayList<>(origin.size() + ids.length);
        merged.addAll(origin);
        for (PlanId id : ids) {
            if (id != null) {
                merged.add(id);
            }
        }
        this.origin = List.copyOf(merged);
        return (PlanType) this;
    }

    /**
     * Query-plan children written to JSON. Usually {@link #children()}.
     * {@code FragmentExec} exposes its inner logical plan here because {@link #children()} is empty.
     */
    public List<? extends QueryPlan<?>> jsonChildren() {
        return children();
    }

    /**
     * When a transform builds a fresh node of this same class, keep this node's id and origin.
     * A node that already sits in {@code original}'s tree (a rewrite that collapses onto a descendant)
     * keeps its own id.
     */
    @Override
    protected void adoptIdentityFrom(Node<?> original) {
        if (original instanceof QueryPlan<?> plan && plan.getClass() == getClass() && sharesInstance(plan, this) == false) {
            copyIdentityFrom(plan);
        }
    }

    private static boolean sharesInstance(QueryPlan<?> root, QueryPlan<?> target) {
        if (root == target) {
            return true;
        }
        List<? extends QueryPlan<?>> children = root.children();
        for (int i = 0; i < children.size(); i++) {
            if (sharesInstance(children.get(i), target)) {
                return true;
            }
        }
        return false;
    }

    /** JSON object for this plan. The skeleton is never truncated. */
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        PlanToXContent.writePlan(builder, this);
        return builder;
    }

    /** Compact JSON, for EXPLAIN cells and tests. */
    public String toJsonString() {
        return toJson(false);
    }

    /** Pretty JSON for the dedicated plan logger. */
    public String toPrettyJsonString() {
        return toJson(true);
    }

    private String toJson(boolean pretty) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            if (pretty) {
                builder.prettyPrint();
            }
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The ordered list of attributes (i.e. columns) this plan produces when executed.
     * Must be called only on resolved plans, otherwise may throw an exception or return wrong results.
     */
    public abstract List<Attribute> output();

    public AttributeSet outputSet() {
        if (lazyOutputSet == null) {
            lazyOutputSet = AttributeSet.of(output());
        }
        return lazyOutputSet;
    }

    public AttributeSet inputSet() {
        if (lazyInputSet == null) {
            lazyInputSet = AttributeSet.of(children(), QueryPlan::output);
        }
        return lazyInputSet;
    }

    /**
     * Returns the top-level expressions for this query plan node.
     * In other words the node properties.
     */
    public List<Expression> expressions() {
        if (lazyExpressions == null) {
            lazyExpressions = computeExpressions();
        }
        return lazyExpressions;
    }

    protected List<Expression> computeExpressions() {
        List<Expression> expressions = new ArrayList<>();
        forEachPropertyOnly(Object.class, e -> doForEachExpression(e, expressions::add));
        return expressions;
    }

    /**
     * The attributes required to be in the {@link QueryPlan#inputSet()} for this plan to be valid.
     * Excludes generated references.
     * <p>
     * E.g. for {@code EVAL x = 2*some_field, y = 2*x} this includes {@code some_field} but neither {@code x} nor {@code y}.
     * For {@code ENRICH some_policy ON field WITH some_enrich_field} this includes {@code field} but excludes the generated reference
     * {@code some_enrich_field}.
     */
    public AttributeSet references() {
        if (lazyReferences == null) {
            lazyReferences = computeReferences();
        }
        return lazyReferences;
    }

    /**
     * This very likely needs to be overridden for {@link QueryPlan#references} to be correct when inheriting.
     * This can be called on unresolved plans and therefore must not rely on calls to {@link QueryPlan#output()}.
     */
    protected AttributeSet computeReferences() {
        return Expressions.references(expressions());
    }

    //
    // pass Object.class as a type token to pick Collections of expressions not just expressions
    //

    public PlanType transformExpressionsOnly(Function<Expression, ? extends Expression> rule) {
        return transformPropertiesOnly(Object.class, e -> doTransformExpression(e, exp -> exp.transformDown(rule)));
    }

    public <E extends Expression> PlanType transformExpressionsOnly(Class<E> typeToken, Function<E, ? extends Expression> rule) {
        return transformPropertiesOnly(Object.class, e -> doTransformExpression(e, exp -> exp.transformDown(typeToken, rule)));
    }

    public <E extends Expression> PlanType transformExpressionsOnlyUp(Class<E> typeToken, Function<E, ? extends Expression> rule) {
        return transformPropertiesOnly(Object.class, e -> doTransformExpression(e, exp -> exp.transformUp(typeToken, rule)));
    }

    public <E extends Expression> PlanType transformExpressionsDown(Class<E> typeToken, Function<E, ? extends Expression> rule) {
        return transformPropertiesDown(Object.class, e -> doTransformExpression(e, exp -> exp.transformDown(typeToken, rule)));
    }

    public <E extends Expression> PlanType transformExpressionsDown(
        Predicate<Node<?>> shouldVisit,
        Class<E> typeToken,
        Function<E, ? extends Expression> rule
    ) {
        return transformDown(
            shouldVisit,
            t -> t.transformNodeProps(Object.class, e -> doTransformExpression(e, exp -> exp.transformDown(typeToken, rule)))
        );
    }

    public <E extends Expression> PlanType transformExpressionsUp(Class<E> typeToken, Function<E, ? extends Expression> rule) {
        return transformPropertiesUp(Object.class, e -> doTransformExpression(e, exp -> exp.transformUp(typeToken, rule)));
    }

    public <E extends Expression> PlanType transformExpressionsUp(
        Predicate<Node<?>> shouldVisit,
        Class<E> typeToken,
        Function<E, ? extends Expression> rule
    ) {
        return transformUp(
            shouldVisit,
            t -> t.transformNodeProps(Object.class, e -> doTransformExpression(e, exp -> exp.transformUp(typeToken, rule)))
        );
    }

    @SuppressWarnings("unchecked")
    private static Object doTransformExpression(Object arg, Function<Expression, ? extends Expression> traversal) {
        if (arg instanceof Expression exp) {
            return traversal.apply(exp);
        }

        // WARNING: if the collection is typed, an incompatible function will be applied to it
        // this results in CCE at runtime and additional filtering is required
        // preserving the type information is hacky and weird (a lot of context needs to be passed around and the lambda itself
        // has no type info so it's difficult to have automatic checking without having base classes).

        if (arg instanceof List<?> c) {
            List<Object> transformed = null;
            boolean hasChanged = false;
            // please do not refactor it to a for-each loop to avoid
            // allocating iterator that performs concurrent modification checks and extra stack frames
            for (int i = 0, size = c.size(); i < size; i++) {
                var e = c.get(i);
                Object next = doTransformExpression(e, traversal);
                if (e.equals(next) == false) {
                    if (hasChanged == false) {
                        hasChanged = true;
                        transformed = new ArrayList<>(c);
                    }
                    transformed.set(i, next);
                }
            }
            return hasChanged ? transformed : arg;
        } else if (arg instanceof Collection<?> c) {
            List<Object> transformed = null;
            boolean hasChanged = false;
            int i = 0;
            for (Object e : c) {
                Object next = doTransformExpression(e, traversal);
                if (e.equals(next) == false) {
                    if (hasChanged == false) {
                        hasChanged = true;
                        // TODO if collection is a set then this silently changes its semantics by allowing duplicates
                        // We should fix it or confirm this branch is never needed
                        transformed = new ArrayList<>(c);
                    }
                    transformed.set(i, next);
                }
                i++;
            }
            return hasChanged ? transformed : arg;
        }

        return arg;
    }

    public void forEachExpression(Consumer<? super Expression> rule) {
        forEachExpression(Expression.class, rule);
    }

    public <E extends Expression> void forEachExpression(Class<E> typeToken, Consumer<? super E> rule) {
        forEachPropertyOnly(Object.class, e -> doForEachExpression(e, exp -> exp.forEachDown(typeToken, rule)));
    }

    public <E extends Expression> void forEachExpressionDown(Class<? extends E> typeToken, Consumer<? super E> rule) {
        forEachPropertyDown(Object.class, e -> doForEachExpression(e, exp -> exp.forEachDown(typeToken, rule)));
    }

    public <E extends Expression> void forEachExpressionUp(Class<E> typeToken, Consumer<? super E> rule) {
        forEachPropertyUp(Object.class, e -> doForEachExpression(e, exp -> exp.forEachUp(typeToken, rule)));
    }

    @SuppressWarnings("unchecked")
    private static void doForEachExpression(Object arg, Consumer<Expression> traversal) {
        if (arg instanceof Expression exp) {
            traversal.accept(exp);
        } else if (arg instanceof Collection<?> c) {
            for (Object o : c) {
                doForEachExpression(o, traversal);
            }
        }
    }
}
