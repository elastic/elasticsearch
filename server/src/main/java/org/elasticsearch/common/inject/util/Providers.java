/*
 * Copyright (C) 2007 Google Inc.
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

package org.elasticsearch.common.inject.util;

import org.elasticsearch.common.inject.Provider;

/**
 * Static utility methods for creating and working with instances of
 * {@link Provider}.
 *
 * @author Kevin Bourrillion (kevinb9n@gmail.com)
 * @since 2.0
 */
public final class Providers {

    private Providers() {
    }

    /**
     * Returns a provider which always provides {@code instance}.  This should not
     * be necessary to use in your application, but is helpful for several types
     * of unit tests.
     *
     * @param instance the instance that should always be provided.  This is also
     *                 permitted to be null, to enable aggressive testing, although in real
     *                 life a Guice-supplied Provider will never return null.
     */
    public static <T> Provider<T> of(final T instance) {
        return new Provider<T>() {
            @Override
            public T get() {
                return instance;
            }

            @Override
            public String toString() {
                return "of(" + instance + ")";
            }
        };
    }
}
