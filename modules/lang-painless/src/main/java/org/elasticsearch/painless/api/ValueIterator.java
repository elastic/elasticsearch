/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * An {@link Iterator} that can return primitive values
 */
public interface ValueIterator<T> extends Iterator<T> {
    boolean nextBoolean();
    byte nextByte();
    short nextShort();
    char nextChar();
    int nextInt();
    long nextLong();
    float nextFloat();
    double nextDouble();

    default void forEachRemaining(IntConsumer action) {
        while (hasNext()) {
            action.accept(nextInt());
        }
    }

    default void forEachRemaining(LongConsumer action) {
        while (hasNext()) {
            action.accept(nextLong());
        }
    }

    default void forEachRemaining(DoubleConsumer action) {
        while (hasNext()) {
            action.accept(nextDouble());
        }
    }

    @Override
    default void forEachRemaining(Consumer<? super T> action) {
        if (action instanceof IntConsumer i) {
            forEachRemaining(i);
        } else if (action instanceof LongConsumer l) {
            forEachRemaining(l);
        } else if (action instanceof DoubleConsumer d) {
            forEachRemaining(d);
        } else {
            Iterator.super.forEachRemaining(action);
        }
    }
}
