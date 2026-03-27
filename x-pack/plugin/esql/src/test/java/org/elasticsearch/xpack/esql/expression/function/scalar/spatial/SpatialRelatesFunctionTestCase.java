/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;

public abstract class SpatialRelatesFunctionTestCase extends BinarySpatialFunctionTestCase {

    protected static void addSpatialCombinations(List<TestCaseSupplier> suppliers, DataType... dataTypes) {
        addSpatialCombinations(suppliers, dataTypes, DataType.BOOLEAN, false);
    }

    protected static void addSpatialGridCombinations(List<TestCaseSupplier> suppliers, DataType... dataTypes) {
        addSpatialGridCombinations(suppliers, dataTypes, DataType.BOOLEAN);
    }
}
