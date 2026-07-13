/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RemoteClusterCredentialsManagerTests extends ESTestCase {
    public void testResolveRemoteClusterCredentials() {
        final String clusterAlias = randomAlphaOfLength(9);
        final String otherClusterAlias = randomAlphaOfLength(10);

        final RemoteClusterCredentialsManager credentialsManager = new RemoteClusterCredentialsManager(Settings.EMPTY);
        {
            final String secret = randomAlphaOfLength(20);
            final Settings settings = buildSettingsWithCredentials(clusterAlias, secret);
            final RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult actual = credentialsManager.updateClusterCredentials(
                settings
            );
            assertThat(actual.addedClusterAliases(), containsInAnyOrder(clusterAlias));
            assertThat(actual.removedClusterAliases(), is(empty()));
            assertThat(credentialsManager.resolveCredentials(clusterAlias).toString(), equalTo(secret));
            assertThat(credentialsManager.hasCredentials(otherClusterAlias), is(false));
        }

        {
            final String updatedSecret = randomAlphaOfLength(21);
            final RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult actual = credentialsManager.updateClusterCredentials(
                buildSettingsWithCredentials(clusterAlias, updatedSecret)
            );
            assertThat(credentialsManager.resolveCredentials(clusterAlias).toString(), equalTo(updatedSecret));
            assertThat(actual.addedClusterAliases(), is(empty()));
            assertThat(actual.removedClusterAliases(), is(empty()));
        }

        {
            final RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult actual = credentialsManager.updateClusterCredentials(
                Settings.EMPTY
            );
            assertThat(actual.addedClusterAliases(), is(empty()));
            assertThat(actual.removedClusterAliases(), containsInAnyOrder(clusterAlias));
            assertThat(credentialsManager.hasCredentials(clusterAlias), is(false));
        }
    }

    public void testUpdateRemoteClusterCredentials() {
        final String clusterAlias = randomAlphaOfLength(9);
        final String otherClusterAlias = randomAlphaOfLength(10);

        final RemoteClusterCredentialsManager credentialsManager = new RemoteClusterCredentialsManager(Settings.EMPTY);

        // addition
        {
            final RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult actual = credentialsManager.updateClusterCredentials(
                buildSettingsWithRandomCredentialsForAliases(clusterAlias, otherClusterAlias)
            );
            assertThat(actual.addedClusterAliases(), containsInAnyOrder(clusterAlias, otherClusterAlias));
            assertThat(actual.removedClusterAliases(), is(empty()));
        }

        // update and removal
        {
            final RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult actual = credentialsManager.updateClusterCredentials(
                buildSettingsWithRandomCredentialsForAliases(clusterAlias)
            );
            assertThat(actual.addedClusterAliases(), is(empty()));
            assertThat(actual.removedClusterAliases(), containsInAnyOrder(otherClusterAlias));
        }

        // addition and removal
        {
            final RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult actual = credentialsManager.updateClusterCredentials(
                buildSettingsWithRandomCredentialsForAliases(otherClusterAlias)
            );
            assertThat(actual.addedClusterAliases(), containsInAnyOrder(otherClusterAlias));
            assertThat(actual.removedClusterAliases(), containsInAnyOrder(clusterAlias));
        }

        // removal
        {
            final RemoteClusterCredentialsManager.UpdateRemoteClusterCredentialsResult actual = credentialsManager.updateClusterCredentials(
                Settings.EMPTY
            );
            assertThat(actual.addedClusterAliases(), is(empty()));
            assertThat(actual.removedClusterAliases(), containsInAnyOrder(otherClusterAlias));
        }
    }

    private Settings buildSettingsWithCredentials(String clusterAlias, String secret) {
        final Settings.Builder builder = Settings.builder();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote." + clusterAlias + ".credentials", secret);
        return builder.setSecureSettings(secureSettings).build();
    }

    private Settings buildSettingsWithRandomCredentialsForAliases(String... clusterAliases) {
        final Settings.Builder builder = Settings.builder();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        for (var alias : clusterAliases) {
            secureSettings.setString("cluster.remote." + alias + ".credentials", randomAlphaOfLength(42));
        }
        return builder.setSecureSettings(secureSettings).build();
    }
}
