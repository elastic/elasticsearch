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
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

/**
 * Injects members of instances of a given type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class MembersInjectorImpl<T> implements MembersInjector<T> {
    private final TypeLiteral<T> typeLiteral;
    private final InjectorImpl injector;
    private final List<SingleMemberInjector> memberInjectors;
    private final List<MembersInjector<? super T>> userMembersInjectors;
    private final List<InjectionListener<? super T>> injectionListeners;

    MembersInjectorImpl(InjectorImpl injector, TypeLiteral<T> typeLiteral,
                        EncounterImpl<T> encounter, List<SingleMemberInjector> memberInjectors) {
        this.injector = injector;
        this.typeLiteral = typeLiteral;
        this.memberInjectors = memberInjectors;
        this.userMembersInjectors = encounter.getMembersInjectors();
        this.injectionListeners = encounter.getInjectionListeners();
    }

    public List<SingleMemberInjector> getMemberInjectors() {
        return memberInjectors;
    }

    @Override
    public void injectMembers(T instance) {
        Errors errors = new Errors(typeLiteral);
        try {
            injectAndNotify(instance, errors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
        }

        errors.throwProvisionExceptionIfErrorsExist();
    }

    void injectAndNotify(final T instance, final Errors errors) throws ErrorsException {
        if (instance == null) {
            return;
        }

        injector.callInContext(new ContextualCallable<Void>() {
            @Override
            public Void call(InternalContext context) throws ErrorsException {
                injectMembers(instance, errors, context);
                return null;
            }
        });

        notifyListeners(instance, errors);
    }

    void notifyListeners(T instance, Errors errors) throws ErrorsException {
        int numErrorsBefore = errors.size();
        for (InjectionListener<? super T> injectionListener : injectionListeners) {
            try {
                injectionListener.afterInjection(instance);
            } catch (RuntimeException e) {
                errors.errorNotifyingInjectionListener(injectionListener, typeLiteral, e);
            }
        }
        errors.throwIfNewErrors(numErrorsBefore);
    }

    void injectMembers(T t, Errors errors, InternalContext context) {
        // optimization: use manual for/each to save allocating an iterator here
        for (int i = 0, size = memberInjectors.size(); i < size; i++) {
            memberInjectors.get(i).inject(errors, context, t);
        }

        // optimization: use manual for/each to save allocating an iterator here
        for (int i = 0, size = userMembersInjectors.size(); i < size; i++) {
            MembersInjector<? super T> userMembersInjector = userMembersInjectors.get(i);
            try {
                userMembersInjector.injectMembers(t);
            } catch (RuntimeException e) {
                errors.errorInUserInjector(userMembersInjector, typeLiteral, e);
            }
        }
    }

    @Override
    public String toString() {
        return "MembersInjector<" + typeLiteral + ">";
    }

    public Set<InjectionPoint> getInjectionPoints() {
        return unmodifiableSet(memberInjectors.stream()
                .map(SingleMemberInjector::getInjectionPoint)
                .collect(toSet()));
    }
}
