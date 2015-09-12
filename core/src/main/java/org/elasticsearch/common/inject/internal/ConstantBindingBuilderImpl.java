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

package org.elasticsearch.common.inject.internal;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.binder.AnnotatedConstantBindingBuilder;
import org.elasticsearch.common.inject.binder.ConstantBindingBuilder;
import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.lang.annotation.Annotation;
import java.util.List;

/**
 * Bind a constant.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public final class ConstantBindingBuilderImpl<T>
        extends AbstractBindingBuilder<T>
        implements AnnotatedConstantBindingBuilder, ConstantBindingBuilder {

    @SuppressWarnings("unchecked") // constant bindings start out with T unknown
    public ConstantBindingBuilderImpl(Binder binder, List<Element> elements, Object source) {
        super(binder, elements, source, (Key<T>) NULL_KEY);
    }

    @Override
    public ConstantBindingBuilder annotatedWith(Class<? extends Annotation> annotationType) {
        annotatedWithInternal(annotationType);
        return this;
    }

    @Override
    public ConstantBindingBuilder annotatedWith(Annotation annotation) {
        annotatedWithInternal(annotation);
        return this;
    }

    @Override
    public void to(final String value) {
        toConstant(String.class, value);
    }

    @Override
    public void to(final int value) {
        toConstant(Integer.class, value);
    }

    @Override
    public void to(final long value) {
        toConstant(Long.class, value);
    }

    @Override
    public void to(final boolean value) {
        toConstant(Boolean.class, value);
    }

    @Override
    public void to(final double value) {
        toConstant(Double.class, value);
    }

    @Override
    public void to(final float value) {
        toConstant(Float.class, value);
    }

    @Override
    public void to(final short value) {
        toConstant(Short.class, value);
    }

    @Override
    public void to(final char value) {
        toConstant(Character.class, value);
    }

    @Override
    public void to(final Class<?> value) {
        toConstant(Class.class, value);
    }

    @Override
    public <E extends Enum<E>> void to(final E value) {
        toConstant(value.getDeclaringClass(), value);
    }

    private void toConstant(Class<?> type, Object instance) {
        // this type will define T, so these assignments are safe
        @SuppressWarnings("unchecked")
        Class<T> typeAsClassT = (Class<T>) type;
        @SuppressWarnings("unchecked")
        T instanceAsT = (T) instance;

        if (keyTypeIsSet()) {
            binder.addError(CONSTANT_VALUE_ALREADY_SET);
            return;
        }

        BindingImpl<T> base = getBinding();
        Key<T> key;
        if (base.getKey().getAnnotation() != null) {
            key = Key.get(typeAsClassT, base.getKey().getAnnotation());
        } else if (base.getKey().getAnnotationType() != null) {
            key = Key.get(typeAsClassT, base.getKey().getAnnotationType());
        } else {
            key = Key.get(typeAsClassT);
        }

        if (instanceAsT == null) {
            binder.addError(BINDING_TO_NULL);
        }

        setBinding(new InstanceBindingImpl<>(
                base.getSource(), key, base.getScoping(), ImmutableSet.<InjectionPoint>of(), instanceAsT));
    }

    @Override
    public String toString() {
        return "ConstantBindingBuilder";
    }
}