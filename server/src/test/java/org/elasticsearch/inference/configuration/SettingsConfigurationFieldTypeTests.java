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

public class SettingsConfigurationFieldTypeTests extends ESTestCase {

    public void testFieldType_WithValidConfigurationFieldTypeString() {
        SettingsConfigurationFieldType fieldType = SettingsConfigurationTestUtils.getRandomConfigurationFieldType();
        assertThat(SettingsConfigurationFieldType.fieldType(fieldType.toString()), equalTo(fieldType));
    }

    public void testFieldType_WithInvalidConfigurationFieldTypeString_ExpectIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> SettingsConfigurationFieldType.fieldType("invalid field type"));
    }

}
