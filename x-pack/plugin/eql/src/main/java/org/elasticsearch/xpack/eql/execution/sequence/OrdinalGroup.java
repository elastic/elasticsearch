/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.eql.execution.search.Ordinal;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/** List of in-flight ordinals for a given key. For fast lookup, typically associated with a stage. */
abstract class OrdinalGroup<E> implements Iterable<Ordinal> {

    private final SequenceKey key;
    private final Function<E, Ordinal> extractor;

    // NB: since the size varies significantly, use a LinkedList
    // Considering the order it might make sense to use a B-Tree+ for faster lookups which should work well with
    // timestamp compression (whose range is known for the current frame).
    private final List<E> elements = new LinkedList<>();

    /**
     * index in the list used for resetting the insertion point
     * it gets reset when dealing with descending queries since the data inserted is ascending in a page
     * but descending compared to the previous stages.
     */
    private int insertPosition = 0;

    private int hashCode = 0;

    private Ordinal start, stop;

    protected OrdinalGroup(SequenceKey key, Function<E, Ordinal> extractor) {
        this.key = key;
        hashCode = key.hashCode();

        this.extractor = extractor;
    }

    SequenceKey key() {
        return key;
    }

    void add(E element) {
        hashCode = 31 * hashCode + Objects.hashCode(element);

        Ordinal ordinal = extractor.apply(element);
        if (start == null) {
            start = ordinal;
        } else if (stop == null) {
            stop = ordinal;
        } else {
            if (start.compareTo(ordinal) > 0) {
                start = ordinal;
            }
            if (stop.compareTo(ordinal) < 0) {
                stop = ordinal;
            }
        }
        // add element at the current position
        elements.add(insertPosition++, element);
    }

    void resetInsertPosition() {
        insertPosition = 0;
    }

    /**
     * Returns the latest element from the group that has its timestamp
     * less than the given argument alongside its position in the list.
     * The element and everything before it is removed.
     */
    E trimBefore(Ordinal ordinal) {
        Tuple<E, Integer> match = findBefore(ordinal);

        // trim
        if (match != null) {
            int pos = match.v2() + 1;
            elements.subList(0, pos).clear();

            // update insert position
            insertPosition = insertPosition - pos;
            if (insertPosition < 0) {
                insertPosition = 0;
            }

            // update min time
            if (elements.isEmpty() == false) {
                start = extractor.apply(elements.get(0));
            } else {
                start = null;
                stop = null;
            }
        }
        return match != null ? match.v1() : null;
    }

    E before(Ordinal ordinal) {
        Tuple<E, Integer> match = findBefore(ordinal);
        return match != null ? match.v1() : null;
    }

    private Tuple<E, Integer> findBefore(Ordinal ordinal) {
        E match = null;
        int matchPos = -1;
        int position = -1;
        for (E element : elements) {
            position++;
            Ordinal o = extractor.apply(element);
            if (o.compareTo(ordinal) < 0) {
                match = element;
                matchPos = position;
            } else {
                break;
            }
        }
        return match != null ? new Tuple<>(match, matchPos) : null;
    }

    boolean isEmpty() {
        return elements.isEmpty();
    }

    @Override
    public Iterator<Ordinal> iterator() {
        return new Iterator<>() {
            final Iterator<E> iter = elements.iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public Ordinal next() {
                return extractor.apply(iter.next());
            }
        };
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        OrdinalGroup<?> other = (OrdinalGroup<?>) obj;
        return Objects.equals(key, other.key)
                && Objects.equals(hashCode, other.hashCode);
    }

    @Override
    public String toString() {
        return format(null, "[{}][{}-{}]({} seqs)", key, start, stop, elements.size());
    }
}