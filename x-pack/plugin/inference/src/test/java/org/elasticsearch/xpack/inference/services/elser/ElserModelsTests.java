/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elser;

import org.elasticsearch.test.ESTestCase;

public class ElserModelsTests extends ESTestCase {

    public static String randomElserModel() {
        return randomFrom(ElserModels.VALID_ELSER_MODEL_IDS);
    }

    public void testIsValidElserModel() {
        assertTrue(ElserModels.isValidModel(randomElserModel()));
    }

    public void testIsInvalidElserModel() {
        assertFalse(ElserModels.isValidModel("invalid"));
    }
}
