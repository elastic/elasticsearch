/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class PlanBuilder {
    private final LogicalPlan currentPlan;
    private final List<Expression> currentExpressions = new ArrayList<>();
    private final List<List<Expression>> currentExpressionLists = new ArrayList<>();

    private PlanBuilder(LogicalPlan plan) {
        this.currentPlan = plan;
    }

    public LogicalPlan build() {
        return currentPlan;
    }

    public PlanBuilder withChild(PlanBuilder child) {
        return withChild(child.currentPlan);
    }

    public PlanBuilder withChild(LogicalPlan child) {
        List<LogicalPlan> children = currentPlan.children();
        if (children.size() == 1 && children.get(0) instanceof MockChild) {
            children = new ArrayList<>();
        }

        children.add(child);
        return new PlanBuilder(currentPlan.replaceChildren(children));
    }

    public PlanBuilder withExpression(Expression expression) {
        currentExpressions.add(expression);

        Holder<Integer> i = new Holder<>(0);
        return new PlanBuilder(currentPlan.transformPropertiesOnly(Expression.class, expr -> {
            int idx = i.get();
            if (idx < currentExpressions.size() == false) {
                return expr;
            }
            i.set(idx + 1);
            return currentExpressions.get(idx);
        }));
    }

    public PlanBuilder withExpressionList(List<Expression> expressions) {
        currentExpressionLists.add(expressions);

        Holder<Integer> i = new Holder<>(0);
        return new PlanBuilder(currentPlan.transformPropertiesOnly(List.class, prop -> {
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

    public static PlanBuilder limit() {
        return new PlanBuilder(new Limit(null, new MockExpression(), new MockChild()));
    }

    public static PlanBuilder relation() {
        return new PlanBuilder(new EsRelation((Source) null, (EsIndex) null, (IndexMode) null));
    }

    public static PlanBuilder localRelation() {
        return new PlanBuilder(new LocalRelation((Source) null, new MockList<>(), null));
    }

    public static PlanBuilder localRelation(List<Attribute> output, LocalSupplier supplier) {
        return new PlanBuilder(new LocalRelation((Source) null, output, supplier));
    }

    public static PlanBuilder filter() {
        return new PlanBuilder(new Filter((Source) null, new MockChild(), new MockExpression()));
    }

    public static PlanBuilder orderBy() {
        return new PlanBuilder(new OrderBy((Source) null, new MockChild(), new MockList<>()));
    }

    public static PlanBuilder topN() {
        return new PlanBuilder(new TopN((Source) null, new MockChild(), new MockList<>(), new MockExpression()));
    }

    interface Ignore {}

    private static class MockChild extends EsRelation implements Ignore {
        MockChild() {
            super(Source.EMPTY, new EsIndex("mock_index", Map.of()), IndexMode.STANDARD);
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
