/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.OnlySurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.inference.InferenceFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FoldablesConvertFunction;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.lang.reflect.Parameter;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.MapMatcher.assertMap;

/**
 * Superclass for tests that verify every {@link FunctionDefinition} registered
 * by a plugin has a corresponding test class.
 */
public abstract class AbstractFunctionsHaveTestsCase extends ESTestCase {

    /**
     * Returns the function definitions to check for test coverage.
     */
    protected abstract Collection<FunctionDefinition> functionDefinitions();

    /**
     * Returns the expected list of known-missing test classes. Return
     * {@link ListMatcher#matchesList()} with no items appended when all tests are present.
     */
    protected abstract ListMatcher knownMissingTests();

    public void testRegisteredFunctionHaveTests() {
        Set<String> errors = new TreeSet<>();
        for (FunctionDefinition def : functionDefinitions()) {
            checkFunctionTestExists(errors, def, "Tests", AbstractFunctionTestCase.class);
            if (takesParameters(def)) {
                checkFunctionTestExists(errors, def, "ErrorTests", ErrorsForCasesWithoutExamplesTestCase.class);
            }
            boolean isSerializable = false == OnlySurrogateExpression.class.isAssignableFrom(def.clazz())
                && false == FoldablesConvertFunction.class.isAssignableFrom(def.clazz())
                && false == InferenceFunction.class.isAssignableFrom(def.clazz());
            if (isSerializable) {
                checkFunctionTestExists(errors, def, "SerializationTests", AbstractExpressionSerializationTests.class);
            }
        }
        assertMap("function test errors", List.copyOf(errors), knownMissingTests());
    }

    private static void checkFunctionTestExists(
        Collection<String> errors,
        FunctionDefinition def,
        String suffix,
        Class<? extends ESTestCase> requiredSuperclass
    ) {
        Class<?> functionClass = def.clazz();
        String testClassName = functionClass.getName() + suffix;
        try {
            Class<?> testClass = Class.forName(testClassName);
            if (requiredSuperclass.isAssignableFrom(testClass) == false) {
                errors.add(testClassName + " doesn't extend " + requiredSuperclass.getSimpleName());
            }
        } catch (ClassNotFoundException e) {
            errors.add(testClassName + " is missing");
        }
    }

    /**
     * Does the function take any parameters?
     */
    private static boolean takesParameters(FunctionDefinition def) {
        for (Parameter p : AbstractFunctionTestCase.constructorWithFunctionInfo(def.clazz()).getParameters()) {
            if (Source.class.isAssignableFrom(p.getType())) {
                continue;
            }
            if (Configuration.class.isAssignableFrom(p.getType())) {
                continue;
            }
            if (Expression.class.isAssignableFrom(p.getType())) {
                return true;
            }
            if (List.class.isAssignableFrom(p.getType())) {
                return true;
            }
            throw new IllegalStateException("unknown argument type " + p);
        }
        return false;
    }
}
