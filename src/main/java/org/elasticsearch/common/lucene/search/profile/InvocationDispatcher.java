/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.search.profile;

/*
        Pilfered from Simon at: https://github.com/s1monw/elasticsearch/commit/bfdc39368d67cdcd0abd328eda6ad44323dde87d
        TODO where should this live?
 */


import org.apache.lucene.util.CollectionUtil;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class InvocationDispatcher<P, R> {

    private final List<Method> dispatchableMethods;
    private final Map<Class<?>, Method> methodByType = new ConcurrentHashMap<Class<?>, Method>();   // TODO Simon, the CopyOnWriteMap as throwing dozens of errors? Replaced with regular map
    private final Class<?> dispatchabelType;
    private final String methodName;
    /**
     * Creates a new {@link InvocationDispatcher} that will dispatch to methods
     * of the given method name, belonging to the given Class and its supertypes
     *
     * @param classType Class whose methods will be dispatched to
     * @param methodName Name of the methods to consider dispatching to
     * @param parameterType Type that parameters will be when dispatched
     * @param returnType Type that the return type of the method's is expected to be
     */
    public InvocationDispatcher(Class<?> classType, String methodName, Class<P> parameterType, Class<R> returnType) {
        this.dispatchableMethods = new ArrayList<Method>();
        dispatchabelType = classType;
        this.methodName = methodName;
        for (; classType != Object.class; classType = classType.getSuperclass()) {
            for (Method method : classType.getDeclaredMethods()) {
                if (isDispatchableMethod(method, methodName, parameterType, returnType)) {
                    dispatchableMethods.add(method);
                    methodByType.put(method.getParameterTypes()[0], method);
                }
            }
        }

        CollectionUtil.timSort(dispatchableMethods, new Comparator<Method>() {

            public int compare(Method o1, Method o2) {
                return isSuperType(o1, o2) ? 1 : -1;
            }
        });
    }

    /**
     * Dispatches the invocation of the most specific method resolveable given
     * the type of parameter.
     *
     * @param walker Object to invoke the method on
     * @param parameter Parameter to dispatch in the method invocation.  Also
     * used to determine the method to dispatch to
     * @return Return value of the method invocation
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws NoSuchMethodException
     */
    @SuppressWarnings("unchecked")
    public final R dispatch(Object walker, P parameter) {
        Class<?> parameterType = parameter.getClass();
        Method method = methodByType.get(parameterType);
        if (method == null) {
            List<Method> possibleMethods = new ArrayList<Method>();
            for (Method dispatchableMethod : dispatchableMethods) {
                if (isSuperType(dispatchableMethod.getParameterTypes()[0], parameterType) && !isSuperType(dispatchableMethod, possibleMethods)) {
                    possibleMethods.add(dispatchableMethod);
                }
            }

            if (possibleMethods.size() == 0) {
                throw new NoMatchingMethodException(dispatchabelType, parameterType, methodName);
            } else if (possibleMethods.size() > 1) {
                throw new AmbigousMethodException(dispatchabelType, parameterType, possibleMethods, methodName);
            }

            method = possibleMethods.get(0);
            methodByType.put(parameterType, method);
        }
        try {
            return (R) method.invoke(walker, parameter);
        } catch (Throwable e) {
            Throwable cause = e.getCause();
            throw propagate(cause == null ? e : cause);
        }
    }

    private RuntimeException propagate(Throwable cause) {
        if (cause == null) {
            throw new NullPointerException(); // safety check
        } else if (cause instanceof Error ) {
            throw ((Error) cause);
        } else if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new RuntimeException(cause);
    }

    /**
     * Determines whether the given method can receive dispatches.
     *
     * @param method Method to check if it can receive dispatches
     * @param methodName Name method's must have to receive dispatches
     * @param parameterType Type that the method's single parameter should be
     * @param returnType Type that the method's return value should be
     * @return {@code true} if the method can receive dispatches, {@code false} otherwise
     */
    private boolean isDispatchableMethod(
            Method method,
            String methodName,
            Class<P> parameterType,
            Class<R> returnType) {
        return methodName.equals(method.getName()) &&
                method.getParameterTypes().length == 1 &&
                isSuperType(parameterType, method.getParameterTypes()[0]) &&
                isSuperType(returnType, method.getReturnType());
    }

    /**
     * Determines if superType is a supertype of subType.
     *
     * @param superType Type to see if its a supertype of subType
     * @param subType Type to see if its a subtype of superType
     * @return {@code true} if superType is the supertype, {@code false}
     *         otherwise
     */
    private boolean isSuperType(Class<?> superType, Class<?> subType) {
        return superType.isAssignableFrom(subType);
    }

    /**
     * Determines if superTypeMethod is a supertype of subTypeMethod.  The
     * supertype relationship is defined as method A is a supertype of
     * method B iff the first parameter if A is a supertype of the first
     * parameter of B.
     *
     * @param superTypeMethod Method to see if its a supertype of the other
     * @param subTypeMethod Method to see if its a subtype of superTypeMethod
     * @return {@code true} if superTypeMethod is a supertype of subTypeMethod,
     *         {@code false} otherwise
     * @see #isSuperType(Class, Class) for parameter supertype relation explaination
     */
    private boolean isSuperType(Method superTypeMethod, Method subTypeMethod) {
        return isSuperType(
                superTypeMethod.getParameterTypes()[0],
                subTypeMethod.getParameterTypes()[0]);
    }

    /**
     * Determines if the given method is a supertype of any of the methods
     * provided in the given list.
     *
     * @param superTypeMethod Method to see if it is a supertype of the others
     * @param methods Methods to check if they are a subtype of the provided
     * supertype method
     * @return {@code true} if superTypeMethod is a supertype of any of the
     *         methods, {@code false} otherwise
     * @see #isSuperType(Method, Method) for Method supertype relation explaination
     */
    private boolean isSuperType(Method superTypeMethod, List<Method> methods) {
        for (Method method : methods) {
            if (isSuperType(superTypeMethod, method)) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("serial")
    public static class AmbigousMethodException extends RuntimeException {
        public AmbigousMethodException(Class<?> visitorClass, Class<?> visitableClass, Collection<Method> matched, String methodName) {
            super(visitorClass.getSimpleName() + " has ambigous "+ methodName + "() methods for " + visitableClass.getName() + ": " + methodsAsString(matched));
        }
    }

    @SuppressWarnings("serial")
    public static class NoMatchingMethodException extends RuntimeException {
        public NoMatchingMethodException(Class<?> visitorClass, Class<?> visitableClass, String methodName) {
            super("No "+ methodName + "() method on " + visitorClass.getSimpleName() + " accepts " + visitableClass.getName());
        }
    }

    public static String methodsAsString(Iterable<Method> methods) {
        StringBuilder builder = new StringBuilder();
        for (Method method : methods) {
            builder.append(method.getName()).append(",");
        }
        return builder.toString();

    }
}