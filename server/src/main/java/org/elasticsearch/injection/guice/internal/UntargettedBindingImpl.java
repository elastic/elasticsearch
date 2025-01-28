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

package org.elasticsearch.injection.guice.internal;

import org.elasticsearch.injection.guice.Injector;
import org.elasticsearch.injection.guice.Key;
import org.elasticsearch.injection.guice.spi.BindingTargetVisitor;
import org.elasticsearch.injection.guice.spi.UntargettedBinding;

public class UntargettedBindingImpl<T> extends BindingImpl<T> implements UntargettedBinding<T> {

    public UntargettedBindingImpl(Injector injector, Key<T> key, Object source) {
        super(injector, key, source, (errors, context, dependency) -> { throw new AssertionError(); }, Scoping.UNSCOPED);
    }

    public UntargettedBindingImpl(Object source, Key<T> key, Scoping scoping) {
        super(source, key, scoping);
    }

    @Override
    public <V> void acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
        visitor.visit(this);
    }

    @Override
    public BindingImpl<T> withEagerSingletonScoping() {
        return new UntargettedBindingImpl<>(getSource(), getKey(), Scoping.EAGER_SINGLETON);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(UntargettedBinding.class).add("key", getKey()).add("source", getSource()).toString();
    }
}
