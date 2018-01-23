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

package org.elasticsearch.common.inject.internal;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

/**
 * Context of a dependency construction. Used to manage circular references.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public class ConstructionContext<T> {

    T currentReference;
    boolean constructing;

    List<DelegatingInvocationHandler<T>> invocationHandlers;

    public T getCurrentReference() {
        return currentReference;
    }

    public void removeCurrentReference() {
        this.currentReference = null;
    }

    public void setCurrentReference(T currentReference) {
        this.currentReference = currentReference;
    }

    public boolean isConstructing() {
        return constructing;
    }

    public void startConstruction() {
        this.constructing = true;
    }

    public void finishConstruction() {
        this.constructing = false;
        invocationHandlers = null;
    }

    public Object createProxy(Errors errors, Class<?> expectedType) throws ErrorsException {
        // TODO: if I create a proxy which implements all the interfaces of
        // the implementation type, I'll be able to get away with one proxy
        // instance (as opposed to one per caller).

        if (!expectedType.isInterface()) {
            throw errors.cannotSatisfyCircularDependency(expectedType).toException();
        }

        if (invocationHandlers == null) {
            invocationHandlers = new ArrayList<>();
        }

        DelegatingInvocationHandler<T> invocationHandler
                = new DelegatingInvocationHandler<>();
        invocationHandlers.add(invocationHandler);

        // ES: Replace, since we don't use bytecode gen, just get the type class loader, or system if its null
        //ClassLoader classLoader = BytecodeGen.getClassLoader(expectedType);
        ClassLoader classLoader = expectedType.getClassLoader() == null ? ClassLoader.getSystemClassLoader() : expectedType.getClassLoader();
        return expectedType.cast(Proxy.newProxyInstance(classLoader,
                new Class[]{expectedType}, invocationHandler));
    }

    public void setProxyDelegates(T delegate) {
        if (invocationHandlers != null) {
            for (DelegatingInvocationHandler<T> handler : invocationHandlers) {
                handler.setDelegate(delegate);
            }
        }
    }

    static class DelegatingInvocationHandler<T> implements InvocationHandler {

        T delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable {
            if (delegate == null) {
                throw new IllegalStateException("This is a proxy used to support"
                        + " circular references involving constructors. The object we're"
                        + " proxying is not constructed yet. Please wait until after"
                        + " injection has completed to use this object.");
            }

            try {
                // This appears to be not test-covered
                return method.invoke(delegate, args);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            } catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }

        void setDelegate(T delegate) {
            this.delegate = delegate;
        }
    }
}
