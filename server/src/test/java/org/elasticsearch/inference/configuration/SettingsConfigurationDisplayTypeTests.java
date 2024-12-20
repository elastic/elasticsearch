/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.inference.SettingsConfigurationTestUtils;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class SettingsConfigurationDisplayTypeTests extends ESTestCase {

    public void testDisplayType_WithValidConfigurationDisplayTypeString() {
        SettingsConfigurationDisplayType displayType = SettingsConfigurationTestUtils.getRandomSettingsConfigurationDisplayType();
        assertThat(SettingsConfigurationDisplayType.displayType(displayType.toString()), equalTo(displayType));
    }

    public void testDisplayType_WithInvalidConfigurationDisplayTypeString_ExpectIllegalArgumentException() {
        expectThrows(
            IllegalArgumentException.class,
            () -> SettingsConfigurationDisplayType.displayType("invalid configuration display type")
        );
    }
}
