/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SecureClusterStateSettingsTests extends ESTestCase {
    MockSecureSettings mockSecureSettings = new MockSecureSettings();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // SecureSettings in cluster state are handled as file settings (get the byte array) both can be fetched as
        // string or file
        mockSecureSettings.setFile("foo", "bar".getBytes(StandardCharsets.UTF_8));
        mockSecureSettings.setFile("goo", "baz".getBytes(StandardCharsets.UTF_8));
    }

    public void testGetSettings() throws Exception {
        SecureClusterStateSettings secureClusterStateSettings = new SecureClusterStateSettings(mockSecureSettings);
        assertThat(secureClusterStateSettings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(secureClusterStateSettings.getString("foo").toString(), equalTo("bar"));
        assertThat(new String(secureClusterStateSettings.getFile("goo").readAllBytes(), StandardCharsets.UTF_8), equalTo("baz"));
    }

    public void testSerialize() throws Exception {
        SecureClusterStateSettings secureClusterStateSettings = new SecureClusterStateSettings(mockSecureSettings);

        final BytesStreamOutput out = new BytesStreamOutput();
        secureClusterStateSettings.writeTo(out);
        final SecureClusterStateSettings fromStream = new SecureClusterStateSettings(out.bytes().streamInput());

        assertThat(fromStream.getSettingNames(), hasSize(2));
        assertThat(fromStream.getSettingNames(), containsInAnyOrder("foo", "goo"));

        assertEquals(secureClusterStateSettings.getString("foo"), fromStream.getString("foo"));
        assertThat(new String(fromStream.getFile("goo").readAllBytes(), StandardCharsets.UTF_8), equalTo("baz"));
        assertTrue(fromStream.isLoaded());
    }
}
