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

package org.elasticsearch.common.collect;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.UnmodifiableIterator;

import java.util.Comparator;
import java.util.Iterator;

public enum Iterators2 {
    ;

    /** Remove duplicated elements from an iterator over sorted content. */
    public static <T> Iterator<T> deduplicateSorted(Iterator<? extends T> iterator, final Comparator<? super T> comparator) {
        // TODO: infer type once JI-9019884 is fixed
        final PeekingIterator<T> it = Iterators.<T>peekingIterator(iterator);
        return new UnmodifiableIterator<T>() {

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public T next() {
                final T ret = it.next();
                while (it.hasNext() && comparator.compare(ret, it.peek()) == 0) {
                    it.next();
                }
                assert !it.hasNext() || comparator.compare(ret, it.peek()) < 0 : "iterator is not sorted: " + ret + " > " + it.peek();
                return ret;
            }

        };
    }

    /** Return a merged view over several iterators, optionally deduplicating equivalent entries. */
    public static <T> Iterator<T> mergeSorted(Iterable<Iterator<? extends T>> iterators, Comparator<? super T> comparator, boolean deduplicate) {
        Iterator<T> it = Iterators.mergeSorted(iterators, comparator);
        if (deduplicate) {
            it = deduplicateSorted(it, comparator);
        }
        return it;
    }

}
