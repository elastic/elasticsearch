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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.InstanceBindingImpl;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.internal.MatcherAndConverter;
import org.elasticsearch.common.inject.internal.SourceProvider;
import org.elasticsearch.common.inject.spi.TypeListenerBinding;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptySet;

/**
 * @author jessewilson@google.com (Jesse Wilson)
 */
class InheritingState implements State {

    private final State parent;

    // Must be a linked hashmap in order to preserve order of bindings in Modules.
    private final Map<Key<?>, Binding<?>> explicitBindingsMutable = new LinkedHashMap<>();
    private final Map<Key<?>, Binding<?>> explicitBindings
            = Collections.unmodifiableMap(explicitBindingsMutable);
    private final Map<Class<? extends Annotation>, Scope> scopes = new HashMap<>();
    private final List<MatcherAndConverter> converters = new ArrayList<>();
    private final List<TypeListenerBinding> listenerBindings = new ArrayList<>();
    private WeakKeySet blacklistedKeys = new WeakKeySet();
    private final Object lock;

    InheritingState(State parent) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.lock = (parent == State.NONE) ? this : parent.lock();
    }

    @Override
    public State parent() {
        return parent;
    }

    @Override
    @SuppressWarnings("unchecked") // we only put in BindingImpls that match their key types
    public <T> BindingImpl<T> getExplicitBinding(Key<T> key) {
        Binding<?> binding = explicitBindings.get(key);
        return binding != null ? (BindingImpl<T>) binding : parent.getExplicitBinding(key);
    }

    @Override
    public Map<Key<?>, Binding<?>> getExplicitBindingsThisLevel() {
        return explicitBindings;
    }

    @Override
    public void putBinding(Key<?> key, BindingImpl<?> binding) {
        explicitBindingsMutable.put(key, binding);
    }

    @Override
    public Scope getScope(Class<? extends Annotation> annotationType) {
        Scope scope = scopes.get(annotationType);
        return scope != null ? scope : parent.getScope(annotationType);
    }

    @Override
    public void putAnnotation(Class<? extends Annotation> annotationType, Scope scope) {
        scopes.put(annotationType, scope);
    }

    @Override
    public Iterable<MatcherAndConverter> getConvertersThisLevel() {
        return converters;
    }

    @Override
    public void addConverter(MatcherAndConverter matcherAndConverter) {
        converters.add(matcherAndConverter);
    }

    @Override
    public MatcherAndConverter getConverter(
            String stringValue, TypeLiteral<?> type, Errors errors, Object source) {
        MatcherAndConverter matchingConverter = null;
        for (State s = this; s != State.NONE; s = s.parent()) {
            for (MatcherAndConverter converter : s.getConvertersThisLevel()) {
                if (converter.getTypeMatcher().matches(type)) {
                    if (matchingConverter != null) {
                        errors.ambiguousTypeConversion(stringValue, source, type, matchingConverter, converter);
                    }
                    matchingConverter = converter;
                }
            }
        }
        return matchingConverter;
    }

    @Override
    public void addTypeListener(TypeListenerBinding listenerBinding) {
        listenerBindings.add(listenerBinding);
    }

    @Override
    public List<TypeListenerBinding> getTypeListenerBindings() {
        List<TypeListenerBinding> parentBindings = parent.getTypeListenerBindings();
        List<TypeListenerBinding> result
                = new ArrayList<>(parentBindings.size() + 1);
        result.addAll(parentBindings);
        result.addAll(listenerBindings);
        return result;
    }

    @Override
    public void blacklist(Key<?> key) {
        parent.blacklist(key);
        blacklistedKeys.add(key);
    }

    @Override
    public boolean isBlacklisted(Key<?> key) {
        return blacklistedKeys.contains(key);
    }

    @Override
    public void clearBlacklisted() {
        blacklistedKeys = new WeakKeySet();
    }

    @Override
    public void makeAllBindingsToEagerSingletons(Injector injector) {
        Map<Key<?>, Binding<?>> x = new LinkedHashMap<>();
        for (Map.Entry<Key<?>, Binding<?>> entry : this.explicitBindingsMutable.entrySet()) {
            Key key = entry.getKey();
            BindingImpl<?> binding = (BindingImpl<?>) entry.getValue();
            Object value = binding.getProvider().get();
            x.put(key, new InstanceBindingImpl<Object>(injector, key, SourceProvider.UNKNOWN_SOURCE, new InternalFactory.Instance(value),
                    emptySet(), value));
        }
        this.explicitBindingsMutable.clear();
        this.explicitBindingsMutable.putAll(x);
    }

    @Override
    public Object lock() {
        return lock;
    }
}
