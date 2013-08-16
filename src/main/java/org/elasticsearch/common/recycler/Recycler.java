/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.recycler;

/**
 */
public abstract class Recycler<T> {

    public static class Sizing<T> extends Recycler<T> {

        private final Recycler<T> recycler;
        private final int smartSize;

        public Sizing(Recycler<T> recycler, int smartSize) {
            super(recycler.c);
            this.recycler = recycler;
            this.smartSize = smartSize;
        }

        @Override
        public void close() {
            recycler.close();
        }

        @Override
        public V<T> obtain(int sizing) {
            if (sizing > 0 && sizing < smartSize) {
                return new NoneRecycler.NV<T>(c.newInstance(sizing));
            }
            return recycler.obtain(sizing);
        }
    }

    public static interface C<T> {

        T newInstance(int sizing);

        void clear(T value);
    }

    public static interface V<T> {

        T v();

        boolean isRecycled();

        void release();
    }

    protected final C<T> c;

    protected Recycler(C<T> c) {
        this.c = c;
    }

    public abstract void close();

    public V<T> obtain() {
        return obtain(-1);
    }

    public abstract V<T> obtain(int sizing);
}
