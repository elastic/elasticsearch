/*
 * Copyright (C) 2006 Google Inc.
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

import org.elasticsearch.injection.guice.internal.ConstructionContext;
import org.elasticsearch.injection.guice.internal.Errors;
import org.elasticsearch.injection.guice.internal.ErrorsException;
import org.elasticsearch.injection.guice.internal.InternalContext;

import java.lang.reflect.InvocationTargetException;

/**
 * Creates instances using an injectable constructor. After construction, all injectable fields and
 * methods are injected.
 *
 * @author crazybob@google.com (Bob Lee)
 */
class ConstructorInjector<T> {

    private final SingleParameterInjector<?>[] parameterInjectors;
    private final ConstructionProxy<T> constructionProxy;
    private final MembersInjectorImpl<T> membersInjector;

    ConstructorInjector(
        ConstructionProxy<T> constructionProxy,
        SingleParameterInjector<?>[] parameterInjectors,
        MembersInjectorImpl<T> membersInjector
    ) {
        this.constructionProxy = constructionProxy;
        this.parameterInjectors = parameterInjectors;
        this.membersInjector = membersInjector;
    }

    /**
     * Construct an instance. Returns {@code Object} instead of {@code T} because
     * it may return a proxy.
     */
    Object construct(Errors errors, InternalContext context, Class<?> expectedType) throws ErrorsException {
        ConstructionContext<T> constructionContext = context.getConstructionContext(this);

        // We have a circular reference between constructors. Return a proxy.
        if (constructionContext.isConstructing()) {
            // TODO (crazybob): if we can't proxy this object, can we proxy the other object?
            return constructionContext.createProxy(errors, expectedType);
        }

        // If we're re-entering this factory while injecting fields or methods,
        // return the same instance. This prevents infinite loops.
        T t = constructionContext.getCurrentReference();
        if (t != null) {
            return t;
        }

        try {
            // First time through...
            constructionContext.startConstruction();
            try {
                Object[] parameters = SingleParameterInjector.getAll(errors, context, parameterInjectors);
                t = constructionProxy.newInstance(parameters);
                constructionContext.setProxyDelegates(t);
            } finally {
                constructionContext.finishConstruction();
            }

            // Store reference. If an injector re-enters this factory, they'll get the same reference.
            constructionContext.setCurrentReference(t);

            membersInjector.injectMembers(t, errors, context);

            return t;
        } catch (InvocationTargetException userException) {
            Throwable cause = userException.getCause() != null ? userException.getCause() : userException;
            throw errors.withSource(constructionProxy.getInjectionPoint()).errorInjectingConstructor(cause).toException();
        } finally {
            constructionContext.removeCurrentReference();
        }
    }
}
