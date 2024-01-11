/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RemoteClusterCredentialsManagerTests extends ESTestCase {
    public void testResolveRemoteClusterCredentials() {
        final String clusterAlias = randomAlphaOfLength(9);
        final String otherClusterAlias = randomAlphaOfLength(10);

        final String secret = randomAlphaOfLength(20);
        final Settings settings = buildSettingsWithCredentials(clusterAlias, secret);
        RemoteClusterCredentialsManager credentialsManager = new RemoteClusterCredentialsManager(settings);
        assertThat(credentialsManager.resolveCredentials(clusterAlias).toString(), equalTo(secret));
        assertThat(credentialsManager.hasCredentials(otherClusterAlias), is(false));

        final String updatedSecret = randomAlphaOfLength(21);
        credentialsManager.updateClusterCredentials(buildSettingsWithCredentials(clusterAlias, updatedSecret));
        assertThat(credentialsManager.resolveCredentials(clusterAlias).toString(), equalTo(updatedSecret));

        credentialsManager.updateClusterCredentials(Settings.EMPTY);
        assertThat(credentialsManager.hasCredentials(clusterAlias), is(false));
    }

    private Settings buildSettingsWithCredentials(String clusterAlias, String secret) {
        final Settings.Builder builder = Settings.builder();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote." + clusterAlias + ".credentials", secret);
        return builder.setSecureSettings(secureSettings).build();
    }
}
