/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.util.Optional;

import static org.elasticsearch.xpack.security.transport.RemoteClusterCredentialsResolver.RemoteClusterCredentials;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RemoteClusterCredentialsResolverTests extends ESTestCase {

    public void testResolveRemoteClusterCredentials() {
        final String clusterNameA = "clusterA";
        final String clusterDoesNotExist = randomAlphaOfLength(10);
        final Settings.Builder builder = Settings.builder();

        final String secret = randomAlphaOfLength(20);
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("cluster.remote." + clusterNameA + ".credentials", secret);
        final Settings settings = builder.setSecureSettings(secureSettings).build();
        RemoteClusterCredentialsResolver remoteClusterAuthorizationResolver = new RemoteClusterCredentialsResolver(settings);
        final Optional<RemoteClusterCredentials> remoteClusterCredentials = remoteClusterAuthorizationResolver.resolve(clusterNameA);
        assertThat(remoteClusterCredentials.isPresent(), is(true));
        assertThat(remoteClusterCredentials.get().clusterAlias(), equalTo(clusterNameA));
        assertThat(remoteClusterCredentials.get().credentials(), equalTo(ApiKeyService.withApiKeyPrefix(secret)));
        assertThat(remoteClusterAuthorizationResolver.resolve(clusterDoesNotExist), is(Optional.empty()));
    }
}
