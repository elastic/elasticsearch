/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.iterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Iterables {

    public static <T> Iterable<T> concat(Iterable<T>... inputs) {
        Objects.requireNonNull(inputs);
        return new ConcatenatedIterable<>(inputs);
    }

    static class ConcatenatedIterable<T> implements Iterable<T> {
        private final Iterable<T>[] inputs;

        ConcatenatedIterable(Iterable<T>[] inputs) {
            this.inputs = Arrays.copyOf(inputs, inputs.length);
        }

        @Override
        public Iterator<T> iterator() {
            return Stream
                    .of(inputs)
                    .map(it -> StreamSupport.stream(it.spliterator(), false))
                    .reduce(Stream::concat)
                    .orElseGet(Stream::empty).iterator();
        }
    }

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
            return StreamSupport
                    .stream(inputs.spliterator(), false)
                    .flatMap(s -> StreamSupport.stream(s.spliterator(), false)).iterator();
        }
    }

    public static <T> T get(Iterable<T> iterable, int position) {
        Objects.requireNonNull(iterable);
        if (position < 0) {
            throw new IllegalArgumentException("position >= 0");
        }
        if (iterable instanceof List) {
            List<T> list = (List<T>)iterable;
            if (position >= list.size()) {
                throw new IndexOutOfBoundsException(Integer.toString(position));
            }
            return list.get(position);
        } else {
            Iterator<T> it = iterable.iterator();
            for (int index = 0; index < position; index++) {
                if (!it.hasNext()) {
                    throw new IndexOutOfBoundsException(Integer.toString(position));
                }
                it.next();
            }
            if (!it.hasNext()) {
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
