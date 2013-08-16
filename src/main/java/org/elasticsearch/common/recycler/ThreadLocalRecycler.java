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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.RamUsageEstimator;

/**
 */
public class ThreadLocalRecycler<T> extends Recycler<T> {

    private final CloseableThreadLocal<Stack<T>> threadLocal = new CloseableThreadLocal<Stack<T>>() {
        @Override
        protected Stack<T> initialValue() {
            return new Stack<T>(stackLimit, Thread.currentThread());
        }
    };

    final int stackLimit;

    public ThreadLocalRecycler(C<T> c, int stackLimit) {
        super(c);
        this.stackLimit = stackLimit;
    }

    @Override
    public void close() {
        threadLocal.close();
    }

    @Override
    public V<T> obtain(int sizing) {
        Stack<T> stack = threadLocal.get();
        T o = stack.pop();
        if (o == null) {
            o = c.newInstance(sizing);
        }
        return new TV<T>(stack, c, o);
    }

    static class TV<T> implements Recycler.V<T> {

        final Stack<T> stack;
        final C<T> c;
        final T value;

        TV(Stack<T> stack, C<T> c, T value) {
            this.stack = stack;
            this.c = c;
            this.value = value;
        }

        @Override
        public T v() {
            return value;
        }

        @Override
        public boolean isRecycled() {
            return true;
        }

        @Override
        public void release() {
            assert Thread.currentThread() == stack.thread;
            c.clear(value);
            stack.push(value);
        }
    }


    static final class Stack<T> {

        final int stackLimit;
        final Thread thread;
        private T[] elements;
        private int size;

        @SuppressWarnings({"unchecked", "SuspiciousArrayCast"})
        Stack(int stackLimit, Thread thread) {
            this.stackLimit = stackLimit;
            this.thread = thread;
            elements = newArray(stackLimit < 10 ? stackLimit : 10);
        }

        T pop() {
            int size = this.size;
            if (size == 0) {
                return null;
            }
            size--;
            T ret = elements[size];
            elements[size] = null;
            this.size = size;
            return ret;
        }

        void push(T o) {
            int size = this.size;
            if (size == elements.length) {
                if (size >= stackLimit) {
                    return;
                }
                T[] newElements = newArray(Math.min(stackLimit, ArrayUtil.oversize(size + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)));
                System.arraycopy(elements, 0, newElements, 0, size);
                elements = newElements;
            }

            elements[size] = o;
            this.size = size + 1;
        }

        @SuppressWarnings({"unchecked", "SuspiciousArrayCast"})
        private static <T> T[] newArray(int length) {
            return (T[]) new Object[length];
        }
    }
}
