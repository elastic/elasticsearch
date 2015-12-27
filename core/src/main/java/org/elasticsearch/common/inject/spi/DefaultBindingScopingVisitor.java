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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.Scope;

import java.lang.annotation.Annotation;

/**
 * No-op visitor for subclassing. All interface methods simply delegate to
 * {@link #visitOther()}, returning its result.
 *
 * @param <V> any type to be returned by the visit method. Use {@link Void} with
 *            {@code return null} if no return type is needed.
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public class DefaultBindingScopingVisitor<V> implements BindingScopingVisitor<V> {

    /**
     * Default visit implementation. Returns {@code null}.
     */
    protected V visitOther() {
        return null;
    }

    @Override
    public V visitEagerSingleton() {
        return visitOther();
    }

    @Override
    public V visitScope(Scope scope) {
        return visitOther();
    }

    @Override
    public V visitScopeAnnotation(Class<? extends Annotation> scopeAnnotation) {
        return visitOther();
    }

    @Override
    public V visitNoScoping() {
        return visitOther();
    }
}
