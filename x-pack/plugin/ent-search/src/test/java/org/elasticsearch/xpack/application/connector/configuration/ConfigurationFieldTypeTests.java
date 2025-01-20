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

public class ConfigurationFieldTypeTests extends ESTestCase {

    public void testFieldType_WithValidConfigurationFieldTypeString() {
        ConfigurationFieldType fieldType = ConnectorTestUtils.getRandomConfigurationFieldType();

        assertThat(ConfigurationFieldType.fieldType(fieldType.toString()), equalTo(fieldType));
    }

    public void testFieldType_WithInvalidConfigurationFieldTypeString_ExpectIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> ConfigurationFieldType.fieldType("invalid field type"));
    }

}
