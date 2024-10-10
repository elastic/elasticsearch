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

public class ConfigurationDisplayTypeTests extends ESTestCase {

    public void testDisplayType_WithValidConfigurationDisplayTypeString() {
        ConfigurationDisplayType displayType = ConnectorTestUtils.getRandomConfigurationDisplayType();

        assertThat(ConfigurationDisplayType.displayType(displayType.toString()), equalTo(displayType));
    }

    public void testDisplayType_WithInvalidConfigurationDisplayTypeString_ExpectIllegalArgumentException() {
        expectThrows(IllegalArgumentException.class, () -> ConfigurationDisplayType.displayType("invalid configuration display type"));
    }
}
