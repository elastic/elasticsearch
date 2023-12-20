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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.FailableCache;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Members injectors by type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class MembersInjectorStore {
    private final InjectorImpl injector;

    private final FailableCache<TypeLiteral<?>, MembersInjectorImpl<?>> cache = new FailableCache<>() {
        @Override
        protected MembersInjectorImpl<?> create(TypeLiteral<?> type, Errors errors) throws ErrorsException {
            return createWithListeners(type, errors);
        }
    };

    MembersInjectorStore(InjectorImpl injector) {
        this.injector = injector;
    }

    /**
     * Returns a new complete members injector with injection listeners registered.
     */
    @SuppressWarnings("unchecked") // the MembersInjector type always agrees with the passed type
    public <T> MembersInjectorImpl<T> get(TypeLiteral<T> key, Errors errors) throws ErrorsException {
        return (MembersInjectorImpl<T>) cache.get(key, errors);
    }

    /**
     * Creates a new members injector and attaches both injection listeners and method aspects.
     */
    private <T> MembersInjectorImpl<T> createWithListeners(TypeLiteral<T> type, Errors errors) throws ErrorsException {
        int numErrorsBefore = errors.size();

        Set<InjectionPoint> injectionPoints;
        try {
            injectionPoints = InjectionPoint.forInstanceMethodsAndFields(type);
        } catch (ConfigurationException e) {
            errors.merge(e.getErrorMessages());
            injectionPoints = e.getPartialValue();
        }
        List<SingleMemberInjector> injectors = getInjectors(injectionPoints, errors);
        errors.throwIfNewErrors(numErrorsBefore);

        return new MembersInjectorImpl<>(injector, type, injectors);
    }

    /**
     * Returns the injectors for the specified injection points.
     */
    List<SingleMemberInjector> getInjectors(Set<InjectionPoint> injectionPoints, Errors errors) {
        List<SingleMemberInjector> injectors = new ArrayList<>();
        for (InjectionPoint injectionPoint : injectionPoints) {
            try {
                Errors errorsForMember = injectionPoint.isOptional() ? new Errors(injectionPoint) : errors.withSource(injectionPoint);
                SingleMemberInjector injector = injectionPoint.getMember() instanceof Field
                    ? new SingleFieldInjector(this.injector, injectionPoint, errorsForMember)
                    : new SingleMethodInjector(this.injector, injectionPoint, errorsForMember);
                injectors.add(injector);
            } catch (ErrorsException ignoredForNow) {
                // ignored for now
            }
        }
        return Collections.unmodifiableList(injectors);
    }
}
