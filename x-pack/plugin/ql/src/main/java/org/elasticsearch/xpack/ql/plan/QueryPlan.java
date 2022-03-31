/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.plan;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * There are two main types of plans, {@code LogicalPlan} and {@code PhysicalPlan}
 */
public abstract class QueryPlan<PlanType extends QueryPlan<PlanType>> extends Node<PlanType> {

    private AttributeSet lazyOutputSet;
    private AttributeSet lazyInputSet;

    public QueryPlan(Source source, List<PlanType> children) {
        super(source, children);
    }

    public abstract List<Attribute> output();

    public AttributeSet outputSet() {
        if (lazyOutputSet == null) {
            lazyOutputSet = new AttributeSet(output());
        }
        return lazyOutputSet;
    }

    public AttributeSet inputSet() {
        if (lazyInputSet == null) {
            List<Attribute> attrs = new ArrayList<>();
            for (PlanType child : children()) {
                attrs.addAll(child.output());
            }
            lazyInputSet = new AttributeSet(attrs);
        }
        return lazyInputSet;
    }

    //
    // pass Object.class as a type token to pick Collections of expressions not just expressions
    //

    public PlanType transformExpressionsOnly(Function<Expression, ? extends Expression> rule) {
        return transformPropertiesOnly(Expression.class, rule);
    }

    public <E extends Expression> PlanType transformExpressionsOnly(Class<E> typeToken, Function<E, ? extends Expression> rule) {
        return transformPropertiesOnly(Object.class, e -> doTransformExpression(e, exp -> exp.transformDown(typeToken, rule)));
    }

    public PlanType transformExpressionsDown(Function<Expression, ? extends Expression> rule) {
        return transformExpressionsDown(Expression.class, rule);
    }

    public <E extends Expression> PlanType transformExpressionsDown(Class<E> typeToken, Function<E, ? extends Expression> rule) {
        return transformPropertiesDown(Object.class, e -> doTransformExpression(e, exp -> exp.transformDown(typeToken, rule)));
    }

    public PlanType transformExpressionsUp(Function<Expression, ? extends Expression> rule) {
        return transformExpressionsUp(Expression.class, rule);
    }

    public <E extends Expression> PlanType transformExpressionsUp(Class<E> typeToken, Function<E, ? extends Expression> rule) {
        return transformPropertiesUp(Object.class, e -> doTransformExpression(e, exp -> exp.transformUp(typeToken, rule)));
    }

    @SuppressWarnings("unchecked")
    private Object doTransformExpression(Object arg, Function<Expression, ? extends Expression> traversal) {
        if (arg instanceof Expression) {
            return traversal.apply((Expression) arg);
        }

        // WARNING: if the collection is typed, an incompatible function will be applied to it
        // this results in CCE at runtime and additional filtering is required
        // preserving the type information is hacky and weird (a lot of context needs to be passed around and the lambda itself
        // has no type info so it's difficult to have automatic checking without having base classes).

        if (arg instanceof Collection<?> c) {
            List<Object> transformed = new ArrayList<>(c.size());
            boolean hasChanged = false;
            for (Object e : c) {
                Object next = doTransformExpression(e, traversal);
                if (e.equals(next)) {
                    // use the initial value
                    next = e;
                } else {
                    hasChanged = true;
                }
                transformed.add(next);
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

    public void forEachExpressionDown(Consumer<? super Expression> rule) {
        forEachExpressionDown(Expression.class, rule);
    }

    public <E extends Expression> void forEachExpressionDown(Class<? extends E> typeToken, Consumer<? super E> rule) {
        forEachPropertyDown(Object.class, e -> doForEachExpression(e, exp -> exp.forEachDown(typeToken, rule)));
    }

    public void forEachExpressionUp(Consumer<? super Expression> rule) {
        forEachExpressionUp(Expression.class, rule);
    }

    public <E extends Expression> void forEachExpressionUp(Class<E> typeToken, Consumer<? super E> rule) {
        forEachPropertyUp(Object.class, e -> doForEachExpression(e, exp -> exp.forEachUp(typeToken, rule)));
    }

    @SuppressWarnings("unchecked")
    private void doForEachExpression(Object arg, Consumer<Expression> traversal) {
        if (arg instanceof Expression) {
            traversal.accept((Expression) arg);
        } else if (arg instanceof Collection<?> c) {
            for (Object o : c) {
                doForEachExpression(o, traversal);
            }
        }
    }
}
