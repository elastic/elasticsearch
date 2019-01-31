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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.internal.Scoping;

import java.lang.annotation.Annotation;
import java.util.Locale;

/**
 * Built-in scope implementations.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class Scopes {

    private Scopes() {
    }

    /**
     * One instance per {@link Injector}. Also see {@code @}{@link Singleton}.
     */
    public static final Scope SINGLETON = new Scope() {
        @Override
        public <T> Provider<T> scope(Key<T> key, final Provider<T> creator) {
            return new Provider<T>() {

                private volatile T instance;

                // DCL on a volatile is safe as of Java 5, which we obviously require.
                @Override
                @SuppressWarnings("DoubleCheckedLocking")
                public T get() {
                    if (instance == null) {
                        /*
                        * Use a pretty coarse lock. We don't want to run into deadlocks
                        * when two threads try to load circularly-dependent objects.
                        * Maybe one of these days we will identify independent graphs of
                        * objects and offer to load them in parallel.
                        */
                        synchronized (InjectorImpl.class) {
                            if (instance == null) {
                                instance = creator.get();
                            }
                        }
                    }
                    return instance;
                }

                @Override
                public String toString() {
                    return String.format(Locale.ROOT, "%s[%s]", creator, SINGLETON);
                }
            };
        }

        @Override
        public String toString() {
            return "Scopes.SINGLETON";
        }
    };

    /**
     * No scope; the same as not applying any scope at all.  Each time the
     * Injector obtains an instance of an object with "no scope", it injects this
     * instance then immediately forgets it.  When the next request for the same
     * binding arrives it will need to obtain the instance over again.
     * <p>
     * This exists only in case a class has been annotated with a scope
     * annotation such as {@link Singleton @Singleton}, and you need to override
     * this to "no scope" in your binding.
     *
     * @since 2.0
     */
    public static final Scope NO_SCOPE = new Scope() {
        @Override
        public <T> Provider<T> scope(Key<T> key, Provider<T> unscoped) {
            return unscoped;
        }

        @Override
        public String toString() {
            return "Scopes.NO_SCOPE";
        }
    };

    /**
     * Scopes an internal factory.
     */
    static <T> InternalFactory<? extends T> scope(Key<T> key, InjectorImpl injector,
                                                  InternalFactory<? extends T> creator, Scoping scoping) {

        if (scoping.isNoScope()) {
            return creator;
        }

        Scope scope = scoping.getScopeInstance();

        // TODO: use diamond operator once JI-9019884 is fixed
        Provider<T> scoped
                = scope.scope(key, new ProviderToInternalFactoryAdapter<T>(injector, creator));
        return new InternalFactoryToProviderAdapter<>(
                Initializables.<Provider<? extends T>>of(scoped));
    }

    /**
     * Replaces annotation scopes with instance scopes using the Injector's annotation-to-instance
     * map. If the scope annotation has no corresponding instance, an error will be added and unscoped
     * will be retuned.
     */
    static Scoping makeInjectable(Scoping scoping, InjectorImpl injector, Errors errors) {
        Class<? extends Annotation> scopeAnnotation = scoping.getScopeAnnotation();
        if (scopeAnnotation == null) {
            return scoping;
        }

        Scope scope = injector.state.getScope(scopeAnnotation);
        if (scope != null) {
            return Scoping.forInstance(scope);
        }

        errors.scopeNotFound(scopeAnnotation);
        return Scoping.UNSCOPED;
    }
}
