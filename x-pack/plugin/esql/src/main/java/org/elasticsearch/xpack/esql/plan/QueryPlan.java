/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;

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

    public QueryPlan(Source source, List<PlanType> children) {
        super(source, children);
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
            List<Attribute> attrs = new ArrayList<>();
            for (PlanType child : children()) {
                attrs.addAll(child.output());
            }
            lazyInputSet = AttributeSet.of(attrs);
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
