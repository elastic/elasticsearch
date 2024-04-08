/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.APMSystemUser;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;

/**
 * Test case with method to handle the starting and stopping the stores for native users and roles
 */
public abstract class NativeRealmIntegTestCase extends SecurityIntegTestCase {

    @Before
    public void ensureNativeStoresStarted() throws Exception {
        assertSecurityIndexActive();
        if (shouldSetReservedUserPasswords()) {
            setupReservedPasswords();
        }
    }

    @After
    public void stopESNativeStores() throws Exception {
        deleteSecurityIndex();

        if (getCurrentClusterScope() == Scope.SUITE) {
            // Clear the realm cache for all realms since we use a SUITE scoped cluster
            getSecurityClient(SECURITY_REQUEST_OPTIONS).clearRealmCache("*");
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // we are randomly running a large number of nodes in these tests so we limit the number of worker threads
        // since the default of 2 * CPU count might use up too much direct memory for thread-local direct buffers for each node's
        // transport threads
        builder.put(Netty4Plugin.WORKER_COUNT.getKey(), random().nextInt(3) + 1);
        return builder.build();
    }

    private SecureString reservedPassword = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    protected SecureString getReservedPassword() {
        return reservedPassword;
    }

    protected boolean shouldSetReservedUserPasswords() {
        return true;
    }

    public void setupReservedPasswords() throws IOException {
        setupReservedPasswords(getRestClient());
    }

    public void setupReservedPasswords(RestClient restClient) throws IOException {
        logger.info("setting up reserved passwords for test");
        {
            Request request = new Request("PUT", "/_security/user/elastic/_password");
            request.setJsonEntity("{\"password\": \"" + new String(reservedPassword.getChars()) + "\"}");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ElasticUser.NAME, BOOTSTRAP_PASSWORD));
            request.setOptions(options);
            restClient.performRequest(request);
        }

        RequestOptions.Builder optionsBuilder = RequestOptions.DEFAULT.toBuilder();
        optionsBuilder.addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(ElasticUser.NAME, reservedPassword));
        RequestOptions options = optionsBuilder.build();
        final List<String> usernames = Arrays.asList(
            KibanaUser.NAME,
            KibanaSystemUser.NAME,
            LogstashSystemUser.NAME,
            BeatsSystemUser.NAME,
            APMSystemUser.NAME,
            RemoteMonitoringUser.NAME
        );
        for (String username : usernames) {
            Request request = new Request("PUT", "/_security/user/" + username + "/_password");
            request.setJsonEntity("{\"password\": \"" + new String(reservedPassword.getChars()) + "\"}");
            request.setOptions(options);
            restClient.performRequest(request);
        }
        logger.info("setting up reserved passwords finished");
    }
}
