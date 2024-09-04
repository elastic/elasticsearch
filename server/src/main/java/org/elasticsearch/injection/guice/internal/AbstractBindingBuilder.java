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

import org.elasticsearch.injection.guice.Binder;
import org.elasticsearch.injection.guice.Key;
import org.elasticsearch.injection.guice.spi.Element;
import org.elasticsearch.injection.guice.spi.InstanceBinding;

import java.util.List;

/**
 * Bind a value or constant.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public abstract class AbstractBindingBuilder<T> {

    public static final String IMPLEMENTATION_ALREADY_SET = "Implementation is set more than once.";
    public static final String SINGLE_INSTANCE_AND_SCOPE = "Setting the scope is not permitted when binding to a single instance.";
    public static final String SCOPE_ALREADY_SET = "Scope is set more than once.";
    public static final String BINDING_TO_NULL = "Binding to null instances is not allowed. "
        + "Use toProvider(Providers.of(null)) if this is your intended behaviour.";

    protected final List<Element> elements;
    protected final int position;
    protected final Binder binder;
    private BindingImpl<T> binding;

    public AbstractBindingBuilder(Binder binder, List<Element> elements, Object source, Key<T> key) {
        this.binder = binder;
        this.elements = elements;
        this.position = elements.size();
        this.binding = new UntargettedBindingImpl<>(source, key, Scoping.UNSCOPED);
        elements.add(position, this.binding);
    }

    protected BindingImpl<T> getBinding() {
        return binding;
    }

    protected BindingImpl<T> setBinding(BindingImpl<T> binding) {
        this.binding = binding;
        elements.set(position, binding);
        return binding;
    }

    public void asEagerSingleton() {
        checkNotScoped();
        setBinding(getBinding().withEagerSingletonScoping());
    }

    protected void checkNotTargetted() {
        if ((binding instanceof UntargettedBindingImpl) == false) {
            binder.addError(IMPLEMENTATION_ALREADY_SET);
        }
    }

    protected void checkNotScoped() {
        // Scoping isn't allowed when we have only one instance.
        if (binding instanceof InstanceBinding) {
            binder.addError(SINGLE_INSTANCE_AND_SCOPE);
            return;
        }

        if (binding.getScoping().isExplicitlyScoped()) {
            binder.addError(SCOPE_ALREADY_SET);
        }
    }
}
