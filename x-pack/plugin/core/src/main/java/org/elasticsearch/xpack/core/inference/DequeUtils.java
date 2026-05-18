/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

public final class DequeUtils {

    private DequeUtils() {
        // util functions only
    }

    public static <T> Deque<T> readDeque(StreamInput in, Writeable.Reader<T> reader) throws IOException {
        return in.readCollection(ArrayDeque::new, ((stream, deque) -> deque.offer(reader.read(in))));
    }

    public static boolean dequeEquals(Deque<?> thisDeque, Deque<?> otherDeque) {
        if (thisDeque.size() != otherDeque.size()) {
            return false;
        }
        var thisIter = thisDeque.iterator();
        var otherIter = otherDeque.iterator();
        while (thisIter.hasNext() && otherIter.hasNext()) {
            if (thisIter.next().equals(otherIter.next()) == false) {
                return false;
            }
        }
        return true;
    }

    public static int dequeHashCode(Deque<?> deque) {
        if (deque == null) {
            return 0;
        }
        return deque.stream().reduce(1, (hashCode, chunk) -> 31 * hashCode + (chunk == null ? 0 : chunk.hashCode()), Integer::sum);
    }

    public static <T> Deque<T> of(T elem) {
        var deque = new ArrayDeque<T>(1);
        deque.offer(elem);
        return deque;
    }
}
