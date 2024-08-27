/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.ann;

/**
 * When supporting multivalued fields, it is necessary to know how to combine them.
 * The combiner.initialize() is called to reset internal state, and then a nested loop (nested for each parameter) is
 * used to call combiner.add(value) on each value calculated from the normal call to the process function.
 * Finally, the combiner.result() method is used to return the result to be appended to the results block.
 * Note that if the accumulated value type is not the same as the final result type, you need to use an additional
 * annotation parameter, <code>mvCombinerResultType</code> to specify the final result type to append to the results block.
 * @param <T> the return type of the method that is annotated by the Evaluator annotation, and passed to the add method.
 * @param <V> the return type of the combiner.result() method, and appending to the results block.
 */
public interface MvCombiner<T, V> {
    void initialize();

    void add(T value);

    V result();

    class MvUnsupported implements MvCombiner<Object, Object> {
        @Override
        public void initialize() {
            throw new UnsupportedOperationException("Multivalued fields are not supported");
        }

        @Override
        public void add(Object value) {
            throw new UnsupportedOperationException("Multivalued fields are not supported");
        }

        @Override
        public Object result() {
            throw new UnsupportedOperationException("Multivalued fields are not supported");
        }
    }
}
