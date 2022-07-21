/*
 * Copyright (C) 2006 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject.internal;

import org.elasticsearch.common.inject.spi.Dependency;

/**
 * Creates objects which will be injected.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public interface InternalFactory<T> {

    /**
     * ES:
     * An factory that returns a pre created instance.
     */
    class Instance<T> implements InternalFactory<T> {

        private final T object;

        public Instance(T object) {
            this.object = object;
        }

        @Override
        public T get(Errors errors, InternalContext context, Dependency<?> dependency) {
            return object;
        }

        @Override
        public String toString() {
            return object.toString();
        }
    }

    /**
     * Creates an object to be injected.
     *
     * @param context of this injection
     * @return instance to be injected
     * @throws org.elasticsearch.common.inject.internal.ErrorsException
     *          if a value cannot be provided
     */
    T get(Errors errors, InternalContext context, Dependency<?> dependency) throws ErrorsException;
}
