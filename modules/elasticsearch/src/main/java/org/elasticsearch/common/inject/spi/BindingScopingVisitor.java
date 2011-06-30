/**
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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.Scope;

import java.lang.annotation.Annotation;

/**
 * Visits each of the strategies used to scope an injection.
 *
 * @param <V> any type to be returned by the visit method. Use {@link Void} with
 *            {@code return null} if no return type is needed.
 * @since 2.0
 */
public interface BindingScopingVisitor<V> {

    /**
     * Visit an eager singleton or single instance. This scope strategy is found on both module and
     * injector bindings.
     */
    V visitEagerSingleton();

    /**
     * Visit a scope instance. This scope strategy is found on both module and injector bindings.
     */
    V visitScope(Scope scope);

    /**
     * Visit a scope annotation. This scope strategy is found only on module bindings. The instance
     * that implements this scope is registered by {@link org.elasticsearch.common.inject.Binder#bindScope(Class,
     * Scope) Binder.bindScope()}.
     */
    V visitScopeAnnotation(Class<? extends Annotation> scopeAnnotation);

    /**
     * Visit an unspecified or unscoped strategy. On a module, this strategy indicates that the
     * injector should use scoping annotations to find a scope. On an injector, it indicates that
     * no scope is applied to the binding. An unscoped binding will behave like a scoped one when it
     * is linked to a scoped binding.
     */
    V visitNoScoping();
}
