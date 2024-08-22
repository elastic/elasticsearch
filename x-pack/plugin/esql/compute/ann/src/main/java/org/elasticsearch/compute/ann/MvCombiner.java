/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

/**
 * When supporting multivalued fields, it is necessary to know how to combine them.
 * This is done using a loop over the fields, calling combiner.combine(previous, value) on each, where the previous
 * value was initialized to combiner.initial() before the loop.
 * @param <T> the return type of the method that is annotated by the Evaluator annotation.
 */
public interface MvCombiner<T> {
    T initial();

    T combine(T previous, T value);

    class MvUnsupported implements MvCombiner<Object> {
        @Override
        public Object initial() {
            throw new UnsupportedOperationException("Multivalued fields are not supported");
        }

        @Override
        public Object combine(Object previous, Object value) {
            throw new UnsupportedOperationException("Multivalued fields are not supported");
        }
    }
}
