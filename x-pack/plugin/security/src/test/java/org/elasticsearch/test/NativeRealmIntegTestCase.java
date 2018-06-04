/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;


import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.client.SecurityClient;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

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
            SecurityClient client = securityClient(internalCluster().transportClient());
            client.prepareClearRealmCache().get();
        }
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    public Set<String> excludeTemplates() {
        Set<String> templates = Sets.newHashSet(super.excludeTemplates());
        templates.add(SecurityIndexManager.SECURITY_TEMPLATE_NAME); // don't remove the security index template
        return templates;
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
            String payload = "{\"password\": \"" + new String(reservedPassword.getChars()) + "\"}";
            HttpEntity entity = new NStringEntity(payload, ContentType.APPLICATION_JSON);
            BasicHeader authHeader = new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                    UsernamePasswordToken.basicAuthHeaderValue(ElasticUser.NAME, BOOTSTRAP_PASSWORD));
            String route = "/_xpack/security/user/elastic/_password";
            Response response = restClient.performRequest("PUT", route, Collections.emptyMap(), entity, authHeader);
            assertEquals(response.getStatusLine().getReasonPhrase(), 200, response.getStatusLine().getStatusCode());
        }

        for (String username : Arrays.asList(KibanaUser.NAME, LogstashSystemUser.NAME, BeatsSystemUser.NAME)) {
            String payload = "{\"password\": \"" + new String(reservedPassword.getChars()) + "\"}";
            HttpEntity entity = new NStringEntity(payload, ContentType.APPLICATION_JSON);
            BasicHeader authHeader = new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                    UsernamePasswordToken.basicAuthHeaderValue(ElasticUser.NAME, reservedPassword));
            String route = "/_xpack/security/user/" + username + "/_password";
            Response response = restClient.performRequest("PUT", route, Collections.emptyMap(), entity, authHeader);
            assertEquals(response.getStatusLine().getReasonPhrase(), 200, response.getStatusLine().getStatusCode());
        }
        logger.info("setting up reserved passwords finished");
    }
}
