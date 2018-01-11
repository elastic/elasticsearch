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

import org.elasticsearch.common.inject.InjectorImpl.MethodInvoker;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InternalContext;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Invokes an injectable method.
 */
class SingleMethodInjector implements SingleMemberInjector {
    final MethodInvoker methodInvoker;
    final SingleParameterInjector<?>[] parameterInjectors;
    final InjectionPoint injectionPoint;

    SingleMethodInjector(InjectorImpl injector, InjectionPoint injectionPoint, Errors errors)
            throws ErrorsException {
        this.injectionPoint = injectionPoint;
        final Method method = (Method) injectionPoint.getMember();
        methodInvoker = createMethodInvoker(method);
        parameterInjectors = injector.getParametersInjectors(injectionPoint.getDependencies(), errors);
    }

    private MethodInvoker createMethodInvoker(final Method method) {

        // We can't use FastMethod if the method is private.
        int modifiers = method.getModifiers();
        if (!Modifier.isPrivate(modifiers) && !Modifier.isProtected(modifiers)) {
        }

        return new MethodInvoker() {
            @Override
            public Object invoke(Object target, Object... parameters)
                    throws IllegalAccessException, InvocationTargetException {
                return method.invoke(target, parameters);
            }
        };
    }

    @Override
    public InjectionPoint getInjectionPoint() {
        return injectionPoint;
    }

    @Override
    public void inject(Errors errors, InternalContext context, Object o) {
        Object[] parameters;
        try {
            parameters = SingleParameterInjector.getAll(errors, context, parameterInjectors);
        } catch (ErrorsException e) {
            errors.merge(e.getErrors());
            return;
        }

        try {
            methodInvoker.invoke(o, parameters);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e); // a security manager is blocking us, we're hosed
        } catch (InvocationTargetException userException) {
            Throwable cause = userException.getCause() != null
                    ? userException.getCause()
                    : userException;
            errors.withSource(injectionPoint).errorInjectingMethod(cause);
        }
    }
}
