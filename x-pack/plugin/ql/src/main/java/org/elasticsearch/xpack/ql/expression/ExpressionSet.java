/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyList;

/**
 * @param <E> expression type
 */
public class ExpressionSet<E extends Expression> implements Set<E> {

    @SuppressWarnings("rawtypes")
    public static final ExpressionSet EMPTY = new ExpressionSet<>(emptyList());

    @SuppressWarnings("unchecked")
    public static <T extends Expression> ExpressionSet<T> emptySet() {
        return (ExpressionSet<T>) EMPTY;
    }

    // canonical to actual/original association
    private final Map<Expression, E> map = new LinkedHashMap<>();

    public ExpressionSet() {
        super();
    }

    public ExpressionSet(Collection<? extends E> c) {
        addAll(c);
    }

    // Returns the equivalent expression (if already exists in the set) or null if none is found
    public E get(Expression e) {
        return map.get(e.canonical());
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (o instanceof Expression) {
            return map.containsKey(((Expression) o).canonical());
        }
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!contains(o)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<E> iterator() {
        return map.values().iterator();
    }

    @Override
    public boolean add(E e) {
        return map.putIfAbsent(e.canonical(), e) == null;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean result = true;
        for (E o : c) {
            result &= add(o);
        }
        return result;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean modified = false;

        Iterator<Expression> keys = map.keySet().iterator();

        while (keys.hasNext()) {
            Expression key = keys.next();
            boolean found = false;
            for (Object o : c) {
                if (o instanceof Expression) {
                    o = ((Expression) o).canonical();
                }
                if (key.equals(o)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                keys.remove();
            }
        }
        return modified;
    }

    @Override
    public boolean remove(Object o) {
        if (o instanceof Expression) {
            return map.remove(((Expression) o).canonical()) != null;
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        for (Object o : c) {
            modified |= remove(o);
        }
        return modified;
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public Object[] toArray() {
        return map.values().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return map.values().toArray(a);
    }

    @Override
    public String toString() {
        return map.toString();
    }
}