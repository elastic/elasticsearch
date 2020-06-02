/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.plan;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.AttributeSet;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Node;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    public PlanType transformExpressionsOnly(Function<? super Expression, ? extends Expression> rule) {
        return transformPropertiesOnly(e -> doTransformExpression(e, exp -> exp.transformDown(rule)), Object.class);
    }

    public PlanType transformExpressionsDown(Function<? super Expression, ? extends Expression> rule) {
        return transformPropertiesDown(e -> doTransformExpression(e, exp -> exp.transformDown(rule)), Object.class);
    }

    public PlanType transformExpressionsUp(Function<? super Expression, ? extends Expression> rule) {
        return transformPropertiesUp(e -> doTransformExpression(e, exp -> exp.transformUp(rule)), Object.class);
    }

    private <E extends Expression> Object doTransformExpression(Object arg, Function<? super Expression, E> traversal) {
        if (arg instanceof Expression) {
            return traversal.apply((Expression) arg);
        }
        if (arg instanceof DataType || arg instanceof Map) {
            return arg;
        }

        // WARNING: if the collection is typed, an incompatible function will be applied to it
        // this results in CCE at runtime and additional filtering is required
        // preserving the type information is hacky and weird (a lot of context needs to be passed around and the lambda itself
        // has no type info so it's difficult to have automatic checking without having base classes).

        if (arg instanceof Collection) {
            Collection<?> c = (Collection<?>) arg;
            List<Object> transformed = new ArrayList<>(c.size());
            boolean hasChanged = false;
            for (Object e : c) {
                Object next = doTransformExpression(e, traversal);
                if (!e.equals(next)) {
                    hasChanged = true;
                }
                else {
                    // use the initial value
                    next = e;
                }
                transformed.add(next);
            }

            return hasChanged ? transformed : arg;
        }

        return arg;
    }

    public void forEachExpressionsDown(Consumer<? super Expression> rule) {
        forEachPropertiesDown(e -> doForEachExpression(e, exp -> exp.forEachDown(rule)), Object.class);
    }

    public void forEachExpressionsUp(Consumer<? super Expression> rule) {
        forEachPropertiesUp(e -> doForEachExpression(e, exp -> exp.forEachUp(rule)), Object.class);
    }

    public void forEachExpressions(Consumer<? super Expression> rule) {
        forEachPropertiesOnly(e -> doForEachExpression(e, rule::accept), Object.class);
    }

    private void doForEachExpression(Object arg, Consumer<? super Expression> traversal) {
        if (arg instanceof Expression) {
            traversal.accept((Expression) arg);
        }
        else if (arg instanceof Collection) {
            Collection<?> c = (Collection<?>) arg;
            for (Object o : c) {
                doForEachExpression(o, traversal);
            }
        }
    }
}
