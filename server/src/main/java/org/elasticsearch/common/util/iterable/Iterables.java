/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.iterable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class Iterables {

    /** Flattens the two level {@code Iterable} into a single {@code Iterable}.  Note that this pre-caches the values from the outer {@code
     *  Iterable}, but not the values from the inner one. */
    public static <T> Iterable<T> flatten(Iterable<? extends Iterable<T>> inputs) {
        Objects.requireNonNull(inputs);
        return new FlattenedIterables<>(inputs);
    }

    static class FlattenedIterables<T> implements Iterable<T> {
        private final Iterable<? extends Iterable<T>> inputs;

        FlattenedIterables(Iterable<? extends Iterable<T>> inputs) {
            List<Iterable<T>> list = new ArrayList<>();
            for (Iterable<T> iterable : inputs) {
                list.add(iterable);
            }
            this.inputs = list;
        }

        @Override
        public Iterator<T> iterator() {
            return StreamSupport.stream(inputs.spliterator(), false).flatMap(s -> StreamSupport.stream(s.spliterator(), false)).iterator();
        }
    }

    public static <T> T get(Iterable<T> iterable, int position) {
        Objects.requireNonNull(iterable);
        if (position < 0) {
            throw new IllegalArgumentException("position >= 0");
        }
        if (iterable instanceof List<T> list) {
            if (position >= list.size()) {
                throw new IndexOutOfBoundsException(Integer.toString(position));
            }
            return list.get(position);
        } else {
            Iterator<T> it = iterable.iterator();
            for (int index = 0; index < position; index++) {
                if (it.hasNext() == false) {
                    throw new IndexOutOfBoundsException(Integer.toString(position));
                }
                it.next();
            }
            if (it.hasNext() == false) {
                throw new IndexOutOfBoundsException(Integer.toString(position));
            }
            return it.next();
        }
    }

    public static <T> int indexOf(Iterable<T> iterable, Predicate<T> predicate) {
        int i = 0;
        for (T element : iterable) {
            if (predicate.test(element)) {
                return i;
            }
            i++;
        }
        return -1;
    }

    public static long size(Iterable<?> iterable) {
        return StreamSupport.stream(iterable.spliterator(), true).count();
    }
}
