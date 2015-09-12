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
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Provides;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.inject.util.Modules;

import java.lang.annotation.Annotation;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Creates bindings to methods annotated with {@literal @}{@link Provides}. Use the scope and
 * binding annotations on the provider method to configure the binding.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 */
public final class ProviderMethodsModule implements Module {
    private final Object delegate;
    private final TypeLiteral<?> typeLiteral;

    private ProviderMethodsModule(Object delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.typeLiteral = TypeLiteral.get(this.delegate.getClass());
    }

    /**
     * Returns a module which creates bindings for provider methods from the given module.
     */
    public static Module forModule(Module module) {
        return forObject(module);
    }

    /**
     * Returns a module which creates bindings for provider methods from the given object.
     * This is useful notably for <a href="http://code.google.com/p/google-gin/">GIN</a>
     */
    public static Module forObject(Object object) {
        // avoid infinite recursion, since installing a module always installs itself
        if (object instanceof ProviderMethodsModule) {
            return Modules.EMPTY_MODULE;
        }

        return new ProviderMethodsModule(object);
    }

    @Override
    public synchronized void configure(Binder binder) {
        for (ProviderMethod<?> providerMethod : getProviderMethods(binder)) {
            providerMethod.configure(binder);
        }
    }

    public List<ProviderMethod<?>> getProviderMethods(Binder binder) {
        List<ProviderMethod<?>> result = new ArrayList<>();
        for (Class<?> c = delegate.getClass(); c != Object.class; c = c.getSuperclass()) {
            for (Method method : c.getDeclaredMethods()) {
                if (method.getAnnotation(Provides.class) != null) {
                    result.add(createProviderMethod(binder, method));
                }
            }
        }
        return result;
    }

    <T> ProviderMethod<T> createProviderMethod(Binder binder, final Method method) {
        binder = binder.withSource(method);
        Errors errors = new Errors(method);

        // prepare the parameter providers
        List<Dependency<?>> dependencies = new ArrayList<>();
        List<Provider<?>> parameterProviders = new ArrayList<>();
        List<TypeLiteral<?>> parameterTypes = typeLiteral.getParameterTypes(method);
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int i = 0; i < parameterTypes.size(); i++) {
            Key<?> key = getKey(errors, parameterTypes.get(i), method, parameterAnnotations[i]);
            dependencies.add(Dependency.get(key));
            parameterProviders.add(binder.getProvider(key));
        }

        @SuppressWarnings("unchecked") // Define T as the method's return type.
                TypeLiteral<T> returnType = (TypeLiteral<T>) typeLiteral.getReturnType(method);

        Key<T> key = getKey(errors, returnType, method, method.getAnnotations());
        Class<? extends Annotation> scopeAnnotation
                = Annotations.findScopeAnnotation(errors, method.getAnnotations());

        for (Message message : errors.getMessages()) {
            binder.addError(message);
        }

        return new ProviderMethod<>(key, method, delegate, ImmutableSet.copyOf(dependencies),
                parameterProviders, scopeAnnotation);
    }

    <T> Key<T> getKey(Errors errors, TypeLiteral<T> type, Member member, Annotation[] annotations) {
        Annotation bindingAnnotation = Annotations.findBindingAnnotation(errors, member, annotations);
        return bindingAnnotation == null ? Key.get(type) : Key.get(type, bindingAnnotation);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ProviderMethodsModule
                && ((ProviderMethodsModule) o).delegate == delegate;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
