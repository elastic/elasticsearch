/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

public class NoneRecycler<T> extends AbstractRecycler<T> {

    public NoneRecycler(C<T> c) {
        super(c);
    }

    @Override
    public V<T> obtain() {
        return new NV<>(c.newInstance());
    }

    public static class NV<T> implements Recycler.V<T> {

        T value;

        NV(T value) {
            this.value = value;
        }

        @Override
        public T v() {
            return value;
        }

        @Override
        public boolean isRecycled() {
            return false;
        }

        @Override
        public void close() {
            if (value == null) {
                throw new IllegalStateException("recycler entry already released...");
            }
            value = null;
        }
    }
}
