/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.inference.ServiceConfigurationTestUtils;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class ServiceConfigurationValidationTypeTests extends ESTestCase {

    public void testValidationType_WithValidConfigurationValidationTypeString() {
        ServiceConfigurationValidationType validationType = ServiceConfigurationTestUtils.getRandomConfigurationValidationType();

        assertThat(ServiceConfigurationValidationType.validationType(validationType.toString()), equalTo(validationType));
    }

    public void testValidationType_WithInvalidConfigurationValidationTypeString_ExpectIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> ServiceConfigurationValidationType.validationType("invalid validation type"));
    }

}
