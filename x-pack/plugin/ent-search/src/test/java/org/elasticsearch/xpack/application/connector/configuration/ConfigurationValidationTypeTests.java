/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.configuration;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.application.connector.ConnectorTestUtils;

import static org.hamcrest.Matchers.equalTo;

public class ConfigurationValidationTypeTests extends ESTestCase {

    public void testValidationType_WithValidConfigurationValidationTypeString() {
        ConfigurationValidationType validationType = ConnectorTestUtils.getRandomConfigurationValidationType();

        assertThat(ConfigurationValidationType.validationType(validationType.toString()), equalTo(validationType));
    }

    public void testValidationType_WithInvalidConfigurationValidationTypeString_ExpectIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> ConfigurationValidationType.validationType("invalid validation type"));
    }

}
