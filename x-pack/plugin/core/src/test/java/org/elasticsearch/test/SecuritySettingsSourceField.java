/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.elasticsearch.common.settings.SecureString;

public final class SecuritySettingsSourceField {
    public static final SecureString TEST_PASSWORD_SECURE_STRING = new SecureString("x-pack-test-password".toCharArray());
    public static final String TEST_PASSWORD = "x-pack-test-password";
    public static final String TEST_INVALID_PASSWORD = "invalid-test-password";

    private SecuritySettingsSourceField() {}
}
