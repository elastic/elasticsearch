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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.internal.Scoping;
import org.elasticsearch.common.inject.internal.ToStringBuilder;
import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.ConstructorBinding;
import org.elasticsearch.common.inject.spi.Dependency;

class ConstructorBindingImpl<T> extends BindingImpl<T> implements ConstructorBinding<T> {

    private final Factory<T> factory;

    private ConstructorBindingImpl(
        Injector injector,
        Key<T> key,
        Object source,
        InternalFactory<? extends T> scopedFactory,
        Scoping scoping,
        Factory<T> factory
    ) {
        super(injector, key, source, scopedFactory, scoping);
        this.factory = factory;
    }

    static <T> ConstructorBindingImpl<T> create(InjectorImpl injector, Key<T> key, Object source, Scoping scoping) {
        Factory<T> factoryFactory = new Factory<>();
        InternalFactory<? extends T> scopedFactory = Scopes.scope(injector, factoryFactory, scoping);
        return new ConstructorBindingImpl<>(injector, key, source, scopedFactory, scoping, factoryFactory);
    }

    public void initialize(InjectorImpl injector, Errors errors) throws ErrorsException {
        factory.constructorInjector = injector.constructors.get(getKey().getTypeLiteral(), errors);
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        if (factory.constructorInjector == null) {
            throw new IllegalStateException("not initialized");
        }
        return visitor.visit();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ConstructorBinding.class).add("key", getKey())
            .add("source", getSource())
            .add("scope", getScoping())
            .toString();
    }

    private static class Factory<T> implements InternalFactory<T> {
        private ConstructorInjector<T> constructorInjector;

        @Override
        @SuppressWarnings("unchecked")
        public T get(Errors errors, InternalContext context, Dependency<?> dependency) throws ErrorsException {
            if (constructorInjector == null) {
                throw new IllegalStateException("Constructor not ready");
            }

            // This may not actually be safe because it could return a super type of T (if that's all the
            // client needs), but it should be OK in practice thanks to the wonders of erasure.
            return (T) constructorInjector.construct(errors, context, dependency.getKey().getRawType());
        }
    }
}
