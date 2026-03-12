/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.filtering;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;

import static org.hamcrest.Matchers.equalTo;

public class FilteringValidationStateTests extends ESTestCase {

    public void testFilteringValidationState_WithValidFilteringValidationStateString() {
        FilteringValidationState validationState = ConnectorTestUtils.getRandomFilteringValidationState();

        assertThat(FilteringValidationState.filteringValidationState(validationState.toString()), equalTo(validationState));
    }

    public void testFilteringValidationState_WithInvalidFilteringValidationStateString_ExpectIllegalArgumentException() {
        assertThrows(
            IllegalArgumentException.class,
            () -> FilteringValidationState.filteringValidationState("invalid filtering validation state")
        );
    }

}
