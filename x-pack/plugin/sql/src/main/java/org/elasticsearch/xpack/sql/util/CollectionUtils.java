/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

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
        if (!left.isEmpty()) {
            list.addAll(left);
        }
        if (!right.isEmpty()) {
            list.addAll(right);
        }
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
            }
            else {
                list.addAll(col);
            }
        }
        return list;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> combine(Collection<? extends T> left, T... entries) {
        List<T> list = new ArrayList<>(left.size() + entries.length);
        if (!left.isEmpty()) {
            list.addAll(left);
        }
        if (entries.length > 0) {
            Collections.addAll(list, entries);
        }
        return list;
    }
}