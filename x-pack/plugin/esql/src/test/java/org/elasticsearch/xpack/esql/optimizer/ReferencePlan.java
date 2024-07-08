/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

class ReferencePlan {
    private final LogicalPlan currentPlan;
    private final List<Expression> currentExpressions = new ArrayList<>();
    private final List<List<Expression>> currentExpressionLists = new ArrayList<>();

    private ReferencePlan(LogicalPlan plan) {
        this.currentPlan = plan;
    }

    public ReferencePlan withChild(ReferencePlan child) {
        return withChild(child.currentPlan);
    }

    public ReferencePlan withChild(LogicalPlan child) {
        List<LogicalPlan> children = currentPlan.children();
        if (children.size() == 1 && children.get(0) instanceof MockChild) {
            children = new ArrayList<>();
        }

        children.add(child);
        return new ReferencePlan(currentPlan.replaceChildren(children));
    }

    public ReferencePlan withExpression(Expression expression) {
        currentExpressions.add(expression);

        Holder<Integer> i = new Holder<>(0);
        return new ReferencePlan(currentPlan.transformPropertiesOnly(Expression.class, expr -> {
            int idx = i.get();
            if (idx < currentExpressions.size() == false) {
                return expr;
            }
            i.set(idx + 1);
            return currentExpressions.get(idx);
        }));
    }

    public ReferencePlan withExpressionList(List<Expression> expressions) {
        currentExpressionLists.add(expressions);

        Holder<Integer> i = new Holder<>(0);
        return new ReferencePlan(currentPlan.transformPropertiesOnly(List.class, prop -> {
            if (prop instanceof ParameterizedType pt
                && pt.getRawType() == List.class
                && pt.getActualTypeArguments()[0].getClass().isAssignableFrom(Expression.class)) {
                int idx = i.get();
                if (idx < currentExpressionLists.size() == false) {
                    return prop;
                }
                i.set(idx + 1);
                return currentExpressionLists.get(idx);
            }
            return prop;
        }));
    }

    public boolean matches(LogicalPlan other) {
        return matches(this.currentPlan, other);
    }

    public static ReferencePlan limit() {
        return new ReferencePlan(new Limit(null, new MockExpression(), new MockChild()));
    }

    public static ReferencePlan relation() {
        return new ReferencePlan(new EsRelation((Source) null, (EsIndex) null, new MockList<>(), (IndexMode) null));
    }

    public static ReferencePlan filter() {
        return new ReferencePlan(new Filter((Source) null, new MockChild(), new MockExpression()));
    }

    public static ReferencePlan orderBy() {
        return new ReferencePlan(new OrderBy((Source) null, new MockChild(), new MockList<>()));
    }

    public static ReferencePlan topN() {
        return new ReferencePlan(new TopN((Source) null, new MockChild(), new MockList<>(), new MockExpression()));
    }

    private static boolean matches(LogicalPlan left, LogicalPlan right) {
        if (left.getClass() != right.getClass()) {
            assert false : "Mismatch in plan types: Expected [" + left.getClass() + "], found [" + right.getClass() + "]";
            return false;
        }

        List<Object> leftProperties = left.nodeProperties();
        List<Object> rightProperties = right.nodeProperties();

        for (int i = 0; i < leftProperties.size(); i++) {
            Object leftProperty = leftProperties.get(i);
            Object rightProperty = rightProperties.get(i);

            // Only check equality if left is not null. This way we can be lenient by simply
            // leaving out properties.
            if (leftProperty == null || leftProperty instanceof Ignore) {
                continue;
            }

            if (rightProperty == null) {
                assert false : "Expected [" + leftProperty.getClass() + "], found null.";
                return false;
            }

            if (leftProperty instanceof LogicalPlan l) {
                if (rightProperty instanceof LogicalPlan rightPlan) {
                    if (matches(l, rightPlan) == false) {
                        return false;
                    }
                } else {
                    assert false
                        : "Mismatch in types: Expected [" + leftProperty.getClass() + "], found [" + rightProperty.getClass() + "]";
                    return false;
                }
            } else if (leftProperty instanceof Expression e) {
                if (rightProperty instanceof Expression rightExpr) {
                    if (e.semanticEquals(rightExpr) == false) {
                        assert false : "Mismatch in expressions: Expected [" + e + "], found [" + rightExpr + "]";
                        return false;
                    }
                } else {
                    return false;
                }
            } else if (leftProperty.equals(rightProperty) == false) {
                assert false : "Mismatch in properties: Expected [" + leftProperty + "], found [" + rightProperty + "]";
                return false;
            }
        }

        return true;
    }

    private interface Ignore {}

    private static class MockChild extends EsRelation implements Ignore {
        MockChild() {
            super(Source.EMPTY, new EsIndex("mock_index", Map.of()), List.of(), IndexMode.STANDARD);
        }
    }

    private static class MockExpression extends Literal implements Ignore {
        MockExpression() {
            super(Source.EMPTY, 0, DataType.NULL);
        }
    }

    private static class MockList<E> implements List<E>, Ignore {
        MockList() {}

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return false;
        }

        @Override
        public Iterator<E> iterator() {
            return null;
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return null;
        }

        @Override
        public boolean add(E e) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends E> c) {
            return false;
        }

        @Override
        public boolean addAll(int index, Collection<? extends E> c) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public void clear() {

        }

        @Override
        public E get(int index) {
            return null;
        }

        @Override
        public E set(int index, E element) {
            return null;
        }

        @Override
        public void add(int index, E element) {

        }

        @Override
        public E remove(int index) {
            return null;
        }

        @Override
        public int indexOf(Object o) {
            return 0;
        }

        @Override
        public int lastIndexOf(Object o) {
            return 0;
        }

        @Override
        public ListIterator<E> listIterator() {
            return null;
        }

        @Override
        public ListIterator<E> listIterator(int index) {
            return null;
        }

        @Override
        public List<E> subList(int fromIndex, int toIndex) {
            return List.of();
        }
    }
}
