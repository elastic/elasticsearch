/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test.enrich;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;

import static org.hamcrest.CoreMatchers.containsString;

public class EnrichSecurityIT extends CommonEnrichRestTestCase {

    public static final String ADMIN_USER = "test_admin";
    public static final String ENRICH_USER = "test_enrich";
    public static final String TEST_PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = enrichCluster("basic", true).rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, TEST_PASSWORD, "superuser", true)
        .user(ENRICH_USER, TEST_PASSWORD, "enrich_user,integ_test_role", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return authRequestHeaderSetting(ENRICH_USER, TEST_PASSWORD);
    }

    @Override
    protected Settings restAdminSettings() {
        return authRequestHeaderSetting(ADMIN_USER, TEST_PASSWORD);
    }

    public void testInsufficientPermissionsOnNonExistentIndex() throws Exception {
        // This test is here because it requires a valid user that has permission to execute policy PUTs but should fail if the user
        // does not have access to read the backing indices used to enrich the data.
        Request request = new Request("PUT", "/some-other-index");
        request.setJsonEntity(Strings.format("""
            {
             "mappings" : {%s}
            }""", createSourceIndexMapping()));
        adminClient().performRequest(request);
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource("some-other-index"));
        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertThat(
            exc.getMessage(),
            containsString("unable to store policy because no indices match with the specified index patterns [some-other-index]")
        );
    }
}
