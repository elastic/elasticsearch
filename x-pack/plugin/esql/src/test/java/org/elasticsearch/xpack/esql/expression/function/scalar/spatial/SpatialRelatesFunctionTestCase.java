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
import java.util.Set;

public abstract class SpatialRelatesFunctionTestCase extends BinarySpatialFunctionTestCase {

    protected static void addSpatialCombinations(List<TestCaseSupplier> suppliers, DataType[] dataTypes) {
        addSpatialCombinations(suppliers, dataTypes, DataType.BOOLEAN, false);
    }

    protected static String typeErrorMessage(boolean includeOrdinal, List<Set<DataType>> validPerPosition, List<DataType> types) {
        return typeErrorMessage(includeOrdinal, validPerPosition, types, false);
    }
}
