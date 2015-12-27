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

package org.elasticsearch.common.inject.internal;

import org.elasticsearch.common.inject.Scope;
import org.elasticsearch.common.inject.Scopes;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.inject.Stage;
import org.elasticsearch.common.inject.binder.ScopedBindingBuilder;
import org.elasticsearch.common.inject.spi.BindingScopingVisitor;

import java.lang.annotation.Annotation;

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
        public <V> V acceptVisitor(BindingScopingVisitor<V> visitor) {
            return visitor.visitNoScoping();
        }

        @Override
        public Scope getScopeInstance() {
            return Scopes.NO_SCOPE;
        }

        @Override
        public String toString() {
            return Scopes.NO_SCOPE.toString();
        }

        @Override
        public void applyTo(ScopedBindingBuilder scopedBindingBuilder) {
            // do nothing
        }
    };

    public static final Scoping SINGLETON_ANNOTATION = new Scoping() {
        @Override
        public <V> V acceptVisitor(BindingScopingVisitor<V> visitor) {
            return visitor.visitScopeAnnotation(Singleton.class);
        }

        @Override
        public Class<? extends Annotation> getScopeAnnotation() {
            return Singleton.class;
        }

        @Override
        public String toString() {
            return Singleton.class.getName();
        }

        @Override
        public void applyTo(ScopedBindingBuilder scopedBindingBuilder) {
            scopedBindingBuilder.in(Singleton.class);
        }
    };

    public static final Scoping SINGLETON_INSTANCE = new Scoping() {
        @Override
        public <V> V acceptVisitor(BindingScopingVisitor<V> visitor) {
            return visitor.visitScope(Scopes.SINGLETON);
        }

        @Override
        public Scope getScopeInstance() {
            return Scopes.SINGLETON;
        }

        @Override
        public String toString() {
            return Scopes.SINGLETON.toString();
        }

        @Override
        public void applyTo(ScopedBindingBuilder scopedBindingBuilder) {
            scopedBindingBuilder.in(Scopes.SINGLETON);
        }
    };

    public static final Scoping EAGER_SINGLETON = new Scoping() {
        @Override
        public <V> V acceptVisitor(BindingScopingVisitor<V> visitor) {
            return visitor.visitEagerSingleton();
        }

        @Override
        public Scope getScopeInstance() {
            return Scopes.SINGLETON;
        }

        @Override
        public String toString() {
            return "eager singleton";
        }

        @Override
        public void applyTo(ScopedBindingBuilder scopedBindingBuilder) {
            scopedBindingBuilder.asEagerSingleton();
        }
    };

    public static Scoping forAnnotation(final Class<? extends Annotation> scopingAnnotation) {
        if (scopingAnnotation == Singleton.class) {
            return SINGLETON_ANNOTATION;
        }

        return new Scoping() {
            @Override
            public <V> V acceptVisitor(BindingScopingVisitor<V> visitor) {
                return visitor.visitScopeAnnotation(scopingAnnotation);
            }

            @Override
            public Class<? extends Annotation> getScopeAnnotation() {
                return scopingAnnotation;
            }

            @Override
            public String toString() {
                return scopingAnnotation.getName();
            }

            @Override
            public void applyTo(ScopedBindingBuilder scopedBindingBuilder) {
                scopedBindingBuilder.in(scopingAnnotation);
            }
        };
    }

    public static Scoping forInstance(final Scope scope) {
        if (scope == Scopes.SINGLETON) {
            return SINGLETON_INSTANCE;
        }

        return new Scoping() {
            @Override
            public <V> V acceptVisitor(BindingScopingVisitor<V> visitor) {
                return visitor.visitScope(scope);
            }

            @Override
            public Scope getScopeInstance() {
                return scope;
            }

            @Override
            public String toString() {
                return scope.toString();
            }

            @Override
            public void applyTo(ScopedBindingBuilder scopedBindingBuilder) {
                scopedBindingBuilder.in(scope);
            }
        };
    }

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
    public boolean isEagerSingleton(Stage stage) {
        if (this == EAGER_SINGLETON) {
            return true;
        }

        if (stage == Stage.PRODUCTION) {
            return this == SINGLETON_ANNOTATION || this == SINGLETON_INSTANCE;
        }

        return false;
    }

    /**
     * Returns the scope instance, or {@code null} if that isn't known for this instance.
     */
    public Scope getScopeInstance() {
        return null;
    }

    /**
     * Returns the scope annotation, or {@code null} if that isn't known for this instance.
     */
    public Class<? extends Annotation> getScopeAnnotation() {
        return null;
    }

    public abstract <V> V acceptVisitor(BindingScopingVisitor<V> visitor);

    public abstract void applyTo(ScopedBindingBuilder scopedBindingBuilder);

    private Scoping() {
    }
}
