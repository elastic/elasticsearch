/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.ql.expression.function.FunctionResolutionStrategy;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunctionTests;
import org.elasticsearch.xpack.sql.expression.function.SqlFunctionResolution;

import java.util.Arrays;
import java.util.List;

public class SqlUnresolvedFunctionTests extends UnresolvedFunctionTests {

    @Override
    protected List<FunctionResolutionStrategy> resolutionStrategies() {
        return Arrays.asList(FunctionResolutionStrategy.DEFAULT, SqlFunctionResolution.DISTINCT, SqlFunctionResolution.EXTRACT);
    }
}
