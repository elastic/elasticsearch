/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;

public abstract class CollectionUtils {

    public static boolean isEmpty(Collection<?> col) {
        return col == null || col.isEmpty();
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> combine(List<? extends T> left, List<? extends T> right) {
        if (right.isEmpty()) {
            return (List<T>) left;
        }
        if (left.isEmpty()) {
            return (List<T>) right;
        }

        List<T> list = new ArrayList<>(left.size() + right.size());
        list.addAll(left);
        list.addAll(right);
        return list;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> combine(Collection<? extends T>... collections) {
        if (org.elasticsearch.common.util.CollectionUtils.isEmpty(collections)) {
            return emptyList();
        }

        List<T> list = new ArrayList<>();
        for (Collection<? extends T> col : collections) {
            // typically AttributeSet which ends up iterating anyway plus creating a redundant array
            if (col instanceof Set) {
                for (T t : col) {
                    list.add(t);
                }
            } else {
                list.addAll(col);
            }
        }
        return list;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> combine(Collection<? extends T> left, T... entries) {
        List<T> list = new ArrayList<>(left.size() + entries.length);
        if (left.isEmpty() == false) {
            list.addAll(left);
        }
        if (entries.length > 0) {
            Collections.addAll(list, entries);
        }
        return list;
    }

    /**
     * Creates a copy of the given collection with the given element prepended.
     *
     * @param collection collection to copy
     * @param element    element to prepend
     */
    @SuppressWarnings("unchecked")
    public static <T> List<T> prependToCopy(T element, Collection<T> collection) {
        T[] result = (T[]) new Object[collection.size() + 1];
        result[0] = element;
        if (collection instanceof ArrayList<T> arrayList && arrayList.size() <= 1_000_000) {
            // Creating an array out of a relatively small ArrayList and copying it is faster than iterating.
            System.arraycopy(arrayList.toArray(), 0, result, 1, result.length - 1);
        } else {
            var i = 1;
            for (T t : collection) {
                result[i++] = t;
            }
        }
        return List.of(result);
    }
}
