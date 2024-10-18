/*
 * Copyright (C) 2009 Google Inc.
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

package org.elasticsearch.injection.guice;

import org.elasticsearch.injection.guice.internal.Errors;
import org.elasticsearch.injection.guice.internal.ErrorsException;
import org.elasticsearch.injection.guice.internal.FailableCache;
import org.elasticsearch.injection.guice.spi.InjectionPoint;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Constructor injectors by type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class ConstructorInjectorStore {
    private final InjectorImpl injector;

    private final FailableCache<TypeLiteral<?>, ConstructorInjector<?>> cache = new FailableCache<>() {
        @Override
        protected ConstructorInjector<?> create(TypeLiteral<?> type, Errors errors) throws ErrorsException {
            return createConstructor(type, errors);
        }
    };

    ConstructorInjectorStore(InjectorImpl injector) {
        this.injector = injector;
    }

    /**
     * Returns a new complete constructor injector with injection listeners registered.
     */
    @SuppressWarnings("unchecked") // the ConstructorInjector type always agrees with the passed type
    public <T> ConstructorInjector<T> get(TypeLiteral<T> key, Errors errors) throws ErrorsException {
        return (ConstructorInjector<T>) cache.get(key, errors);
    }

    private <T> ConstructorInjector<T> createConstructor(TypeLiteral<T> type, Errors errors) throws ErrorsException {
        int numErrorsBefore = errors.size();

        InjectionPoint injectionPoint;
        try {
            injectionPoint = InjectionPoint.forConstructorOf(type);
        } catch (ConfigurationException e) {
            errors.merge(e.getErrorMessages());
            throw errors.toException();
        }

        SingleParameterInjector<?>[] constructorParameterInjectors = injector.getParametersInjectors(
            injectionPoint.getDependencies(),
            errors
        );
        MembersInjectorImpl<T> membersInjector = injector.membersInjectorStore.get(type, errors);

        errors.throwIfNewErrors(numErrorsBefore);

        @SuppressWarnings("unchecked") // the injection point is for a constructor of T
        final Constructor<T> constructor = (Constructor<T>) injectionPoint.getMember();
        return new ConstructorInjector<>(new ConstructionProxy<>() {
            @Override
            public T newInstance(Object... arguments) throws InvocationTargetException {
                try {
                    return constructor.newInstance(arguments);
                } catch (InstantiationException e) {
                    throw new AssertionError(e); // shouldn't happen, we know this is a concrete type
                } catch (IllegalAccessException e) {
                    // a security manager is blocking us, we're hosed
                    throw new AssertionError("Wrong access modifiers on " + constructor, e);
                }
            }

            @Override
            public InjectionPoint getInjectionPoint() {
                return injectionPoint;
            }

        }, constructorParameterInjectors, membersInjector);
    }
}
