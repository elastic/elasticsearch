/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.test.ESTestCase;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class MockSecureSettingsTests extends ESTestCase {
    public void testToSecureClusterStateSettings() {
        var mockSettings = new MockSecureSettings();
        assertThat(mockSettings.toSecureClusterStateSettings(), equalTo(SecureClusterStateSettings.EMPTY));

        mockSettings.setString("string", "abc");
        mockSettings.setFile("file", "123".getBytes(UTF_8));

        var clusterStateSettings = mockSettings.toSecureClusterStateSettings();
        assertThat(clusterStateSettings.getSettingNames(), containsInAnyOrder("string", "file"));
        assertThat(clusterStateSettings.getString("string").toString(), equalTo("abc"));
        assertThat(clusterStateSettings.getString("file").toString(), equalTo("123"));
    }
}
