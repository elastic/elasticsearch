/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Runs {@link OrphanRemovalVerificationGoldenTestsHelper} by extending it and setting the required
 * property in {@link BeforeClass}, so no manual -D flags are needed.
 */
public class OrphanRemovalVerificationGoldenTests extends OrphanRemovalVerificationGoldenTestsHelper {

    @BeforeClass
    public static void setOrphanVerificationFlag() {
        System.setProperty(ENABLE_PROPERTY, "true");
    }

    @Override
    @Test
    public void testKeepMe() {
        super.testKeepMe();
    }

    @Override
    @Test
    public void testDeleteMe() {
        super.testDeleteMe();
    }
}
