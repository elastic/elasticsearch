/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.lucene.util.LuceneTestCase.BadApple;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;

import java.util.List;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;

/**
 * This test makes sure that if an action is a cluster action (according to our
 * internal categorization in shield), then we apply the cluster priv checks and don't
 * fallback on the indices privs at all. In particular, this is useful when we want to treat
 * actions that are normally categorized as index actions as cluster actions - for example,
 * index template actions.
 */
//test is just too slow, please fix it to not be sleep-based
@BadApple(bugUrl = "https://github.com/elastic/x-plugins/issues/1007")
public class PermissionPrecedenceTests extends ShieldIntegTestCase {
    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("test123".toCharArray())));

    @Override
    protected String configRoles() {
        return "admin:\n" +
                "  cluster: all\n" +
                "  indices:\n" +
                "    '*': all\n" +
                "\n" +
                "transport_client:\n" +
                "  cluster:\n" +
                "    - cluster:monitor/nodes/info\n" +
                "    - cluster:monitor/state\n" +
                "\n" +
                "user:\n" +
                "  indices:\n" +
                "    'test_*': all\n";
    }

    @Override
    protected String configUsers() {
        return "admin:" + USERS_PASSWD_HASHED + "\n" +
                "client:" + USERS_PASSWD_HASHED + "\n" +
                "user:" + USERS_PASSWD_HASHED + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return "admin:admin\n" +
                "transport_client:client\n" +
                "user:user\n";
    }

    @Override
    protected String nodeClientUsername() {
        return "admin";
    }

    @Override
    protected SecuredString nodeClientPassword() {
        return new SecuredString("test123".toCharArray());
    }

    @Override
    protected String transportClientUsername() {
        return "admin";
    }

    @Override
    protected SecuredString transportClientPassword() {
        return new SecuredString("test123".toCharArray());
    }

    public void testDifferentCombinationsOfIndices() throws Exception {
        Client client = internalCluster().transportClient();

        // first lets try with "admin"... all should work

        PutIndexTemplateResponse putResponse = client.admin().indices().preparePutTemplate("template1")
            .setTemplate("test_*")
            .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, basicAuthHeaderValue(transportClientUsername(), transportClientPassword()))
            .get();
        assertAcked(putResponse);

        GetIndexTemplatesResponse getResponse = client.admin().indices().prepareGetTemplates("template1")
                .get();
        List<IndexTemplateMetaData> templates = getResponse.getIndexTemplates();
        assertThat(templates, hasSize(1));

        // now lets try with "user"

        try {
            client.admin().indices().preparePutTemplate("template1")
                    .setTemplate("test_*")
                    .putHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, basicAuthHeaderValue("user", transportClientPassword()))
                    .get();
            fail("expected an authorization exception as template APIs should require cluster ALL permission");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthorizationException(e);
        }

        try {
            client.admin().indices().prepareGetTemplates("template1")
                    .putHeader("Authorization", basicAuthHeaderValue("user", SecuredStringTests.build("test123")))
                    .get();
            fail("expected an authorization exception as template APIs should require cluster ALL permission");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthorizationException(e);
        }
    }
}
