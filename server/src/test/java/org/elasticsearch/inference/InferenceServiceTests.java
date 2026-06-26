/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

public class InferenceServiceTests extends ESTestCase {

    private static final String ERROR_MESSAGE = "some error";

    public void testSupported_HasNullErrorMessageAndIsSupported() {
        var compatibility = InferenceService.ClusterCompatibility.supported();
        assertNull(compatibility.errorMessage());
        assertTrue(compatibility.isSupported());
    }

    public void testUnsupported_HasErrorMessageAndIsNotSupported() {
        var compatibility = InferenceService.ClusterCompatibility.unsupported(ERROR_MESSAGE);
        assertThat(compatibility.errorMessage(), is(ERROR_MESSAGE));
        assertFalse(compatibility.isSupported());
    }

    public void testSupported_ReturnsCachedSingleton() {
        assertThat(InferenceService.ClusterCompatibility.supported(), sameInstance(InferenceService.ClusterCompatibility.supported()));
    }

    public void testCheckClusterCompatibility_DefaultImplementationReturnsSupported() {
        // InferenceService has many abstract methods; a full stub would be impractical
        // for a default method that ignores all of its parameters, so we use a mock with
        // CALLS_REAL_METHODS to dispatch the default implementation directly.
        var service = mock(InferenceService.class, CALLS_REAL_METHODS);
        var compatibility = service.checkClusterCompatibility(null, null, randomFrom(TaskType.values()), null);
        assertTrue(compatibility.isSupported());
        assertNull(compatibility.errorMessage());
    }
}
