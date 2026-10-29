/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.iceberg;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class IcebergSourceOperatorFactoryTests extends ESTestCase {

    /**
     * {@code describe()} is rendered into {@code profile.drivers}, which any user who may query the dataset can see,
     * so it names the table but not the warehouse location.
     */
    public void testDescribeNamesTheTableButNotItsLocation() {
        IcebergSourceOperatorFactory factory = new IcebergSourceOperatorFactory(
            Runnable::run,
            "s3://secret-bucket/warehouse/db/events",
            null,
            "iceberg",
            null,
            null,
            List.of(),
            1000,
            10
        );

        String description = factory.describe();

        assertTrue(description, description.contains("events"));
        assertFalse(description, description.contains("secret-bucket"));
        assertFalse(description, description.contains("warehouse"));
    }
}
