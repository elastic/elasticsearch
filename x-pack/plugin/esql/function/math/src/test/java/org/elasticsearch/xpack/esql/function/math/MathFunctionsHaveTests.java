/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.function.math;

import org.elasticsearch.test.ListMatcher;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionsHaveTestsCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.ListMatcher.matchesList;

public class MathFunctionsHaveTests extends AbstractFunctionsHaveTestsCase {
    @Override
    protected Collection<FunctionDefinition> functionDefinitions() {
        return Arrays.asList(new MathFunctions().functions());
    }

    @Override
    protected ListMatcher knownMissingTests() {
        return matchesList();
    }
}
