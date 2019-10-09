/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.common;

import java.util.concurrent.atomic.AtomicReference;

public class SetOnce<T> {

    private final class Wrapper {
        private T object;

        private Wrapper(T object) {
            this.object = object;
        }
    }

    public static final class AlreadySetException extends IllegalStateException {
        private AlreadySetException() {
            super("The object cannot be set twice!");
        }
    }

    private final AtomicReference<Wrapper> ref;

    public SetOnce(T object) {
        ref = new AtomicReference<>(new Wrapper(object));
    }

    public SetOnce() {
        ref = new AtomicReference<>();
    }

    public boolean trySet(T object) {
        return ref.compareAndSet(null, new Wrapper(object));
    }

    public void set(T object) {
        if (trySet(object) == false) {
            throw new AlreadySetException();
        }
    }

    public T get() {
        Wrapper wrapper = ref.get();
        return wrapper == null ? null : wrapper.object;
    }
}
