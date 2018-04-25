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

package org.elasticsearch.common.inject.internal;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.ProviderKeyBinding;

public final class LinkedProviderBindingImpl<T>
        extends BindingImpl<T> implements ProviderKeyBinding<T> {

    final Key<? extends Provider<? extends T>> providerKey;

    public LinkedProviderBindingImpl(Injector injector, Key<T> key, Object source,
                                     InternalFactory<? extends T> internalFactory, Scoping scoping,
                                     Key<? extends Provider<? extends T>> providerKey) {
        super(injector, key, source, internalFactory, scoping);
        this.providerKey = providerKey;
    }

    LinkedProviderBindingImpl(Object source, Key<T> key, Scoping scoping,
                              Key<? extends Provider<? extends T>> providerKey) {
        super(source, key, scoping);
        this.providerKey = providerKey;
    }

    @Override
    public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Key<? extends Provider<? extends T>> getProviderKey() {
        return providerKey;
    }

    @Override
    public BindingImpl<T> withScoping(Scoping scoping) {
        return new LinkedProviderBindingImpl<>(getSource(), getKey(), scoping, providerKey);
    }

    @Override
    public BindingImpl<T> withKey(Key<T> key) {
        return new LinkedProviderBindingImpl<>(getSource(), key, getScoping(), providerKey);
    }

    @Override
    public void applyTo(Binder binder) {
        getScoping().applyTo(binder.withSource(getSource())
                .bind(getKey()).toProvider(getProviderKey()));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(ProviderKeyBinding.class)
                .add("key", getKey())
                .add("source", getSource())
                .add("scope", getScoping())
                .add("provider", providerKey)
                .toString();
    }
}
