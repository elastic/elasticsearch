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

package org.elasticsearch.util.guice.inject.internal;

import org.elasticsearch.util.guice.inject.Binder;
import org.elasticsearch.util.guice.inject.Injector;
import org.elasticsearch.util.guice.inject.Key;
import org.elasticsearch.util.guice.inject.spi.BindingTargetVisitor;
import org.elasticsearch.util.guice.inject.spi.Dependency;
import org.elasticsearch.util.guice.inject.spi.ExposedBinding;
import org.elasticsearch.util.guice.inject.spi.PrivateElements;
import org.elasticsearch.util.gcommon.collect.ImmutableSet;

import java.util.Set;

public class ExposedBindingImpl<T> extends BindingImpl<T> implements ExposedBinding<T> {

  private final PrivateElements privateElements;

  public ExposedBindingImpl(Injector injector, Object source, Key<T> key,
      InternalFactory<T> factory, PrivateElements privateElements) {
    super(injector, key, source, factory, Scoping.UNSCOPED);
    this.privateElements = privateElements;
  }

  public ExposedBindingImpl(Object source, Key<T> key, Scoping scoping,
      PrivateElements privateElements) {
    super(source, key, scoping);
    this.privateElements = privateElements;
  }

  public <V> V acceptTargetVisitor(BindingTargetVisitor<? super T, V> visitor) {
    return visitor.visit(this);
  }

  public Set<Dependency<?>> getDependencies() {
    return ImmutableSet.<Dependency<?>>of(Dependency.get(Key.get(Injector.class)));
  }

  public PrivateElements getPrivateElements() {
    return privateElements;
  }

  public BindingImpl<T> withScoping(Scoping scoping) {
    return new ExposedBindingImpl<T>(getSource(), getKey(), scoping, privateElements);
  }

  public ExposedBindingImpl<T> withKey(Key<T> key) {
    return new ExposedBindingImpl<T>(getSource(), key, getScoping(), privateElements);
  }

  @Override public String toString() {
    return new ToStringBuilder(ExposedBinding.class)
        .add("key", getKey())
        .add("source", getSource())
        .add("privateElements", privateElements)
        .toString();
  }

  public void applyTo(Binder binder) {
    throw new UnsupportedOperationException("This element represents a synthetic binding.");
  }
}
