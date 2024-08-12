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

    public void testIsValidModel() {
        assertTrue(ElserModels.isValidModel(randomElserModel()));
    }

    public void testIsValidEisModel() {
        assertTrue(ElserModels.isValidEisModel(ElserModels.ELSER_V2_MODEL));
    }

    public void testIsInvalidModel() {
        assertFalse(ElserModels.isValidModel("invalid"));
    }

    public void testIsInvalidEisModel() {
        assertFalse(ElserModels.isValidEisModel(ElserModels.ELSER_V2_MODEL_LINUX_X86));
    }
}
