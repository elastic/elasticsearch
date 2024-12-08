/*
 * Copyright (C) 2008 Google Inc.
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

package org.elasticsearch.injection.guice.internal;

import org.elasticsearch.injection.guice.Scope;
import org.elasticsearch.injection.guice.Scopes;

/**
 * References a scope, either directly (as a scope instance), or indirectly (as a scope annotation).
 * The scope's eager or laziness is also exposed.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public abstract class Scoping {

    /**
     * No scoping annotation has been applied. Note that this is different from {@code
     * in(Scopes.NO_SCOPE)}, where the 'NO_SCOPE' has been explicitly applied.
     */
    public static final Scoping UNSCOPED = new Scoping() {

        @Override
        public Scope getScopeInstance() {
            return Scopes.NO_SCOPE;
        }

        @Override
        public String toString() {
            return Scopes.NO_SCOPE.toString();
        }

    };

    public static final Scoping EAGER_SINGLETON = new Scoping() {

        @Override
        public Scope getScopeInstance() {
            return Scopes.SINGLETON;
        }

        @Override
        public String toString() {
            return "eager singleton";
        }

    };

    /**
     * Returns true if this scope was explicitly applied. If no scope was explicitly applied then the
     * scoping annotation will be used.
     */
    public boolean isExplicitlyScoped() {
        return this != UNSCOPED;
    }

    /**
     * Returns true if this is the default scope. In this case a new instance will be provided for
     * each injection.
     */
    public boolean isNoScope() {
        return getScopeInstance() == Scopes.NO_SCOPE;
    }

    /**
     * Returns true if this scope is a singleton that should be loaded eagerly in {@code stage}.
     */
    public boolean isEagerSingleton() {
        return this == EAGER_SINGLETON;
    }

    /**
     * Returns the scope instance, or {@code null} if that isn't known for this instance.
     */
    public Scope getScopeInstance() {
        return null;
    }

    private Scoping() {}
}
