/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;

/**
 * Helper containing golden test logic for orphan removal verification. Not run directly by CI
 * (excluded by naming); only invoked by {@link OrphanRemovalVerificationGoldenTests}.
 */
public class OrphanRemovalVerificationGoldenTestsHelper extends GoldenTestCase {

    static final String ENABLE_PROPERTY = "golden.verify.orphan.removal";

    @Before
    public void requireOrphanVerificationFlag() {
        assumeTrue(
            "Only run via OrphanRemovalVerificationGoldenTests which sets " + ENABLE_PROPERTY,
            System.getProperty(ENABLE_PROPERTY) != null
        );
    }

    @Test
    public void testKeepMe() {
        runGoldenTest("FROM employees | LIMIT 1", EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION));
    }

    @Test
    public void testDeleteMe() {
        runGoldenTest("FROM employees | EVAL x = 1 | LIMIT 1", EnumSet.of(Stage.ANALYSIS, Stage.LOGICAL_OPTIMIZATION));
    }
}
