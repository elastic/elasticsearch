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

package org.elasticsearch.injection.guice;

import org.elasticsearch.injection.guice.internal.InternalFactory;
import org.elasticsearch.injection.guice.internal.Scoping;

/**
 * Built-in scope implementations.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class Scopes {

    private Scopes() {}

    /**
     * Scopes an internal factory.
     */
    static <T> InternalFactory<? extends T> scope(InjectorImpl injector, InternalFactory<? extends T> creator, Scoping scoping) {
        return switch (scoping) {
            case UNSCOPED -> creator;
            case EAGER_SINGLETON -> new InternalFactoryToProviderAdapter<>(Initializables.of(new Provider<>() {

                private volatile T instance;

                @Override
                public T get() {
                    if (instance == null) {
                        /*
                         * Use a pretty coarse lock. We don't want to run into deadlocks
                         * when two threads try to load circularly-dependent objects.
                         * Maybe one of these days we will identify independent graphs of
                         * objects and offer to load them in parallel.
                         */
                        synchronized (injector) {
                            if (instance == null) {
                                instance = new ProviderToInternalFactoryAdapter<>(injector, creator).get();
                            }
                        }
                    }
                    return instance;
                }

                @Override
                public String toString() {
                    return creator + "[SINGLETON]";
                }
            }));
        };
    }

}
