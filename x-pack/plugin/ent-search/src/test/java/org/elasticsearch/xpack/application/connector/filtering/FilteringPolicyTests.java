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

public class FilteringPolicyTests extends ESTestCase {

    public void testFilteringPolicy_WithValidFilteringPolicyString() {
        FilteringPolicy filteringPolicy = ConnectorTestUtils.getRandomFilteringPolicy();

        assertThat(FilteringPolicy.filteringPolicy(filteringPolicy.toString()), equalTo(filteringPolicy));
    }

    public void testFilteringPolicy_WithInvalidFilteringPolicyString_ExpectIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> FilteringPolicy.filteringPolicy("invalid filtering policy"));
    }

}
