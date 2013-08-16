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
public class NoneRecycler<T> extends Recycler<T> {

    public NoneRecycler(C<T> c) {
        super(c);
    }

    @Override
    public V<T> obtain(int sizing) {
        return new NV<T>(c.newInstance(sizing));
    }

    @Override
    public void close() {

    }

    public static class NV<T> implements Recycler.V<T> {

        final T value;

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
        public void release() {
        }
    }
}

