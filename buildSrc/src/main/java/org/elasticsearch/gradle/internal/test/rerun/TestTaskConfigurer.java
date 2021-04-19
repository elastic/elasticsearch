/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun;

import org.elasticsearch.gradle.internal.test.rerun.executer.RetryTestExecuter;
import org.gradle.api.Action;
import org.gradle.api.Task;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.testing.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public final class TestTaskConfigurer {

    private TestTaskConfigurer() {}

    public static void configureTestTask(Test test, ObjectFactory objectFactory) {
        TestRerunTaskExtension extension = test.getExtensions()
            .create(TestRerunTaskExtension.NAME, TestRerunTaskExtension.class, objectFactory);
        test.doFirst(new InitTaskAction(extension));
        test.doLast(new FinalizeTaskAction());
    }

    private static RetryTestExecuter createRetryTestExecuter(Task task, TestRerunTaskExtension extension) {
        TestExecuter<JvmTestExecutionSpec> delegate = getTestExecuter(task);
        return new RetryTestExecuter(extension, delegate);
    }

    private static TestExecuter<JvmTestExecutionSpec> getTestExecuter(Task task) {
        return invoke(declaredMethod(Test.class, "createTestExecuter"), task);
    }

    private static void setTestExecuter(Task task, RetryTestExecuter retryTestExecuter) {
        invoke(declaredMethod(Test.class, "setTestExecuter", TestExecuter.class), task, retryTestExecuter);
    }

    private static class FinalizeTaskAction implements Action<Task> {
        @Override
        public void execute(Task task) {
            TestExecuter<JvmTestExecutionSpec> testExecuter = getTestExecuter(task);
            if (testExecuter instanceof RetryTestExecuter) {
                ((RetryTestExecuter) testExecuter).reportJvmCrashDetails();
            } else {
                throw new IllegalStateException("Unexpected test executer: " + testExecuter);
            }
        }
    }

    private static class InitTaskAction implements Action<Task> {

        private final TestRerunTaskExtension extension;

        InitTaskAction(TestRerunTaskExtension extension) {
            this.extension = extension;
        }

        @Override
        public void execute(Task task) {
            RetryTestExecuter retryTestExecuter = createRetryTestExecuter(task, extension);
            setTestExecuter(task, retryTestExecuter);
        }
    }

    private static Method declaredMethod(Class<?> type, String methodName, Class<?>... paramTypes) {
        try {
            return makeAccessible(type.getDeclaredMethod(methodName, paramTypes));
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Method makeAccessible(Method method) {
        method.setAccessible(true);
        return method;
    }

    private static <T> T invoke(Method method, Object instance, Object... args) {
        try {
            Object result = method.invoke(instance, args);
            @SuppressWarnings("unchecked")
            T cast = (T) result;
            return cast;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

}
