/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.logging.internal;

import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class LoggerImplDelegationTests extends ESTestCase {
    public void testAllPublicMethodsDelegateToLog4j() throws InvocationTargetException, IllegalAccessException {
        List<Method> methods = getPublicMethods(Logger.class)
            // skip methods which do not simply pass arguments (perform argument mapping)
            // those are covered in LoggerImplMappingTests
            .filter(m -> m.getName().equals("log") == false)
            .filter(m -> Arrays.asList(m.getParameterTypes()).contains(Supplier.class) == false)
            .filter(m -> Arrays.asList(m.getParameterTypes()).contains(Level.class) == false)
            .collect(Collectors.toList());

        for (Method method : methods) {
            org.apache.logging.log4j.Logger log4jMock = Mockito.mock(org.apache.logging.log4j.Logger.class);
            Logger logger = new LoggerImpl(log4jMock);

            Object[] args = mockedParameters(method);

            method.invoke(logger, args);

            Method log4jMethod = findDelegatedMethodInLog4j(method);
            log4jMethod.invoke(Mockito.verify(log4jMock), args);
        }
    }

    private Method findDelegatedMethodInLog4j(Method esMethod) {
        List<Method> collect = getPublicMethods(org.apache.logging.log4j.Logger.class).filter(m -> m.getName().equals(esMethod.getName()))
            .filter(m -> Arrays.equals(m.getParameterTypes(), esMethod.getParameterTypes()))
            .collect(Collectors.toList());
        assertThat(collect.size(), equalTo(1));
        return collect.get(0);
    }

    private Object[] mockedParameters(Method method) {
        Class<?>[] parameterTypes = method.getParameterTypes();
        return Arrays.stream(parameterTypes).map(p -> {
            if (p.equals(Level.class)) {
                return Mockito.spy(Level.INFO);
            } else if (p.equals(org.apache.logging.log4j.Level.class)) {
                return Mockito.spy(org.apache.logging.log4j.Level.INFO);
            } else {
                return Mockito.any(p);
            }
        }).toArray();
    }

    private Stream<Method> getPublicMethods(Class<?> loggerClass) {
        Method[] methods = loggerClass.getMethods();
        return Arrays.stream(methods).filter(m -> Modifier.isPublic(m.getModifiers()));
    }
}
