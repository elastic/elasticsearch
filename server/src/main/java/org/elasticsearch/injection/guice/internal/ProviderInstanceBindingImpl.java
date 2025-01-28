/*
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.injection.guice.internal;

import org.elasticsearch.injection.guice.Injector;
import org.elasticsearch.injection.guice.Key;
import org.elasticsearch.injection.guice.Provider;
import org.elasticsearch.injection.guice.spi.BindingTargetVisitor;
import org.elasticsearch.injection.guice.spi.InjectionPoint;
import org.elasticsearch.injection.guice.spi.ProviderInstanceBinding;

import java.util.Set;

public final class ProviderInstanceBindingImpl<T> extends BindingImpl<T> implements ProviderInstanceBinding<T> {

    final Provider<? extends T> providerInstance;
    final Set<InjectionPoint> injectionPoints;

    public ProviderInstanceBindingImpl(
        Injector injector,
        Key<T> key,
        Object source,
        InternalFactory<? extends T> internalFactory,
        Scoping scoping,
        Provider<? extends T> providerInstance,
        Set<InjectionPoint> injectionPoints
    ) {
        super(injector, key, source, internalFactory, scoping);
        this.providerInstance = providerInstance;
        this.injectionPoints = injectionPoints;
    }

    public ProviderInstanceBindingImpl(
        Object source,
        Key<T> key,
        Scoping scoping,
        Set<InjectionPoint> injectionPoints,
        Provider<? extends T> providerInstance
    ) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.providerInstance = providerInstance;
    }

    @Override
    public <V> void acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        visitor.visit(this);
    }

    @Override
    public Provider<? extends T> getProviderInstance() {
        return providerInstance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public BindingImpl<T> withEagerSingletonScoping() {
        return new ProviderInstanceBindingImpl<>(getSource(), getKey(), Scoping.EAGER_SINGLETON, injectionPoints, providerInstance);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ProviderInstanceBinding.class).add("key", getKey())
            .add("source", getSource())
            .add("scope", getScoping())
            .add("provider", providerInstance)
            .toString();
    }
}
