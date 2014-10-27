/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.junit.Test;

import java.util.List;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;

/**
 * This test makes sure that if an action is a cluster action (according to our
 * internal categorization in shield, then we apply the cluster priv checks and don't
 * fallback on the indices privs at all. In particular, this is useful when we want to treat
 * actions that are normally categorized as index actions as cluster actions - for example,
 * index template actions.
 */
public class PermissionPrecedenceTests extends ShieldIntegrationTest {

    static final String ROLES =
            "admin:\n" +
            "  cluster: all\n" +
            "  indices:\n" +
            "    '*': all\n" +
            "\n" +
            "user:\n" +
            "  indices:\n" +
            "    'test_*': all\n";

    static final String USERS =
            "admin:{plain}test123\n" +
            "user:{plain}test123\n";

    static final String USERS_ROLES =
            "admin:admin\n" +
            "user:user\n";

    @Override
    protected String configRole() {
        return ROLES;
    }

    @Override
    protected String configUsers() {
        return USERS;
    }

    @Override
    protected String configUsersRoles() {
        return USERS_ROLES;
    }

    @Override
    protected String getClientUsername() {
        return "admin";
    }

    @Override
    protected SecuredString getClientPassword() {
        return SecuredStringTests.build("test123");
    }

    @Test
    public void testDifferetCombinationsOfIndices() throws Exception {

        ClusterService clusterService = internalCluster().getInstance(ClusterService.class);
        TransportAddress address = clusterService.localNode().address();

        TransportClient client = new TransportClient(ImmutableSettings.builder()
                .put("cluster.name", internalCluster().getClusterName())
                .put("node.mode", "network")
                .put(getSSLSettingsForStore("/org/elasticsearch/shield/transport/ssl/certs/simple/testclient.jks", "testclient")))
                .addTransportAddress(address);

        // first lets try with "admin"... all should work

        PutIndexTemplateResponse putResponse = client.admin().indices().preparePutTemplate("template1")
                .setTemplate("test_*")
                .putHeader("Authorization", basicAuthHeaderValue("admin", SecuredStringTests.build("test123")))
                .get();
        assertAcked(putResponse);

        GetIndexTemplatesResponse getResponse = client.admin().indices().prepareGetTemplates("template1")
                .putHeader("Authorization", basicAuthHeaderValue("admin", SecuredStringTests.build("test123")))
                .get();
        List<IndexTemplateMetaData> templates = getResponse.getIndexTemplates();
        assertThat(templates, hasSize(1));

        // now lets try with "user"

        try {
            client.admin().indices().preparePutTemplate("template1")
                    .setTemplate("test_*")
                    .putHeader("Authorization", basicAuthHeaderValue("user", SecuredStringTests.build("test123")))
                    .get();
            fail("expected an authorization exception as template APIs should require cluster ALL permission");
        } catch (AuthorizationException ae) {
            // expected;
        }

        try {
            client.admin().indices().prepareGetTemplates("template1")
                    .putHeader("Authorization", basicAuthHeaderValue("user", SecuredStringTests.build("test123")))
                    .get();
            fail("expected an authorization exception as template APIs should require cluster ALL permission");
        } catch (AuthorizationException ae) {
            // expected
        }
    }
}
