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

import org.apache.lucene.util.CloseableThreadLocal;

import java.lang.ref.SoftReference;

/**
 */
public class SoftThreadLocalRecycler<T> extends Recycler<T> {

    private final CloseableThreadLocal<SoftReference<ThreadLocalRecycler.Stack<T>>> threadLocal = new CloseableThreadLocal<SoftReference<ThreadLocalRecycler.Stack<T>>>();

    final int stackLimit;

    public SoftThreadLocalRecycler(C<T> c, int stackLimit) {
        super(c);
        this.stackLimit = stackLimit;
    }

    @Override
    public void close() {
        threadLocal.close();
    }

    @Override
    public V<T> obtain(int sizing) {
        SoftReference<ThreadLocalRecycler.Stack<T>> ref = threadLocal.get();
        ThreadLocalRecycler.Stack<T> stack = (ref == null) ? null : ref.get();
        if (stack == null) {
            stack = new ThreadLocalRecycler.Stack<T>(stackLimit, Thread.currentThread());
            threadLocal.set(new SoftReference<ThreadLocalRecycler.Stack<T>>(stack));
        }

        T o = stack.pop();
        if (o == null) {
            o = c.newInstance(sizing);
        }
        return new ThreadLocalRecycler.TV<T>(stack, c, o);
    }
}
