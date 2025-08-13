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
import org.elasticsearch.injection.guice.internal.InternalContext;

import java.util.List;

/**
 * Injects members of instances of a given type.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class MembersInjectorImpl<T> implements MembersInjector<T> {
    private final TypeLiteral<T> typeLiteral;
    private final InjectorImpl injector;
    private final List<SingleMethodInjector> memberInjectors;

    MembersInjectorImpl(InjectorImpl injector, TypeLiteral<T> typeLiteral, List<SingleMethodInjector> memberInjectors) {
        this.injector = injector;
        this.typeLiteral = typeLiteral;
        this.memberInjectors = memberInjectors;
    }

    void injectAndNotify(final T instance, final Errors errors) throws ErrorsException {
        if (instance == null) {
            return;
        }

        injector.callInContext((ContextualCallable<Void>) context -> {
            injectMembers(instance, errors, context);
            return null;
        });
    }

    void injectMembers(T t, Errors errors, InternalContext context) {
        // optimization: use manual for/each to save allocating an iterator here
        for (int i = 0, size = memberInjectors.size(); i < size; i++) {
            memberInjectors.get(i).inject(errors, context, t);
        }
    }

    @Override
    public String toString() {
        return "MembersInjector<" + typeLiteral + ">";
    }

}
