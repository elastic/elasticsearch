/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.license.License;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;

import java.util.List;

public abstract class SpatialAggregationTestCase extends AbstractAggregationTestCase {

    /**
     * All spatial aggregations have the same licensing requirements, which is that the function itself is not licensed, but
     * the field types are. Aggregations over shapes are licensed under platinum, while aggregations over points are licensed under basic.
     * @param fieldTypes (null for the function itself, otherwise a map of field named to types)
     * @return The license requirement for the function with that type signature
     */
    protected static License.OperationMode licenseRequirement(List<DataType> fieldTypes) {
        if (fieldTypes == null || fieldTypes.isEmpty()) {
            // The function itself is not licensed, but the field types are.
            return License.OperationMode.BASIC;
        }
        if (fieldTypes.stream().anyMatch(DataType::isSpatialShape)) {
            // Only aggregations over shapes are licensed under platinum.
            return License.OperationMode.PLATINUM;
        }
        // All other field types are licensed under basic.
        return License.OperationMode.BASIC;
    }
}
