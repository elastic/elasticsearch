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

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.PrivateBinder;
import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.ElementVisitor;
import org.elasticsearch.common.inject.spi.PrivateElements;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

/**
 * @author jessewilson@google.com (Jesse Wilson)
 */
public final class PrivateElementsImpl implements PrivateElements {

    /*
    * This class acts as both a value object and as a builder. When getElements() is called, an
    * immutable collection of elements is constructed and the original mutable list is nulled out.
    * Similarly, the exposed keys are made immutable on access.
    */

    private final Object source;

    private List<Element> elementsMutable = new ArrayList<>();
    private List<ExposureBuilder<?>> exposureBuilders = new ArrayList<>();

    /**
     * lazily instantiated
     */
    private List<Element> elements;

    /**
     * lazily instantiated
     */
    private Map<Key<?>, Object> exposedKeysToSources;
    private Injector injector;

    public PrivateElementsImpl(Object source) {
        this.source = Objects.requireNonNull(source, "source");
    }

    @Override
    public Object getSource() {
        return source;
    }

    @Override
    public List<Element> getElements() {
        if (elements == null) {
            elements = Collections.unmodifiableList(elementsMutable);
            elementsMutable = null;
        }

        return elements;
    }

    @Override
    public Injector getInjector() {
        return injector;
    }

    public void initInjector(Injector injector) {
        if (this.injector != null) {
            throw new IllegalStateException("injector already initialized");
        }
        this.injector = Objects.requireNonNull(injector, "injector");
    }

    @Override
    public Set<Key<?>> getExposedKeys() {
        if (exposedKeysToSources == null) {
            Map<Key<?>, Object> exposedKeysToSourcesMutable = new LinkedHashMap<>();
            for (ExposureBuilder<?> exposureBuilder : exposureBuilders) {
                exposedKeysToSourcesMutable.put(exposureBuilder.getKey(), exposureBuilder.getSource());
            }
            exposedKeysToSources = unmodifiableMap(exposedKeysToSourcesMutable);
            exposureBuilders = null;
        }

        return exposedKeysToSources.keySet();
    }

    @Override
    public <T> T acceptVisitor(ElementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public List<Element> getElementsMutable() {
        return elementsMutable;
    }

    public void addExposureBuilder(ExposureBuilder<?> exposureBuilder) {
        exposureBuilders.add(exposureBuilder);
    }

    @Override
    public void applyTo(Binder binder) {
        PrivateBinder privateBinder = binder.withSource(source).newPrivateBinder();

        for (Element element : getElements()) {
            element.applyTo(privateBinder);
        }

        getExposedKeys(); // ensure exposedKeysToSources is populated
        for (Map.Entry<Key<?>, Object> entry : exposedKeysToSources.entrySet()) {
            privateBinder.withSource(entry.getValue()).expose(entry.getKey());
        }
    }

    @Override
    public Object getExposedSource(Key<?> key) {
        getExposedKeys(); // ensure exposedKeysToSources is populated
        Object source = exposedKeysToSources.get(key);
        if (source == null) {
            throw new IllegalArgumentException(key + " not exposed by " + ".");
        }
        return source;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(PrivateElements.class)
                .add("exposedKeys", getExposedKeys())
                .add("source", getSource())
                .toString();
    }
}
