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

import org.elasticsearch.injection.guice.Injector;
import org.elasticsearch.injection.guice.Key;
import org.elasticsearch.injection.guice.Provider;
import org.elasticsearch.injection.guice.spi.BindingTargetVisitor;
import org.elasticsearch.injection.guice.spi.InjectionPoint;
import org.elasticsearch.injection.guice.spi.InstanceBinding;
import org.elasticsearch.injection.guice.util.Providers;

import java.util.Set;

public class InstanceBindingImpl<T> extends BindingImpl<T> implements InstanceBinding<T> {

    final T instance;
    final Provider<T> provider;
    final Set<InjectionPoint> injectionPoints;

    public InstanceBindingImpl(
        Injector injector,
        Key<T> key,
        Object source,
        InternalFactory<? extends T> internalFactory,
        Set<InjectionPoint> injectionPoints,
        T instance
    ) {
        super(injector, key, source, internalFactory, Scoping.UNSCOPED);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    public InstanceBindingImpl(Object source, Key<T> key, Scoping scoping, Set<InjectionPoint> injectionPoints, T instance) {
        super(source, key, scoping);
        this.injectionPoints = injectionPoints;
        this.instance = instance;
        this.provider = Providers.of(instance);
    }

    @Override
    public Provider<T> getProvider() {
        return this.provider;
    }

    @Override
    public <V> void acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        visitor.visit(this);
    }

    @Override
    public T getInstance() {
        return instance;
    }

    @Override
    public Set<InjectionPoint> getInjectionPoints() {
        return injectionPoints;
    }

    @Override
    public BindingImpl<T> withEagerSingletonScoping() {
        return new InstanceBindingImpl<>(getSource(), getKey(), Scoping.EAGER_SINGLETON, injectionPoints, instance);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(InstanceBinding.class).add("key", getKey())
            .add("source", getSource())
            .add("instance", instance)
            .toString();
    }
}
