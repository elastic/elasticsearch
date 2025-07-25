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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public class EnrichSecurityFailureIT extends ESRestTestCase {

    public static final String ADMIN_USER = "test_admin";
    public static final String ENRICH_USER = "test_enrich_no_privs";
    public static final String TEST_PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = CommonEnrichRestTestCase.enrichCluster("basic", true)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, TEST_PASSWORD, "superuser", true)
        .user(ENRICH_USER, TEST_PASSWORD, "enrich_no_privs", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return CommonEnrichRestTestCase.authRequestHeaderSetting(ENRICH_USER, TEST_PASSWORD);
    }

    @Override
    protected Settings restAdminSettings() {
        return CommonEnrichRestTestCase.authRequestHeaderSetting(ADMIN_USER, TEST_PASSWORD);
    }

    public void testFailure() throws Exception {
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(CommonEnrichRestTestCase.generatePolicySource("my-source-index"));
        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertTrue(exc.getMessage().contains("action [cluster:admin/xpack/enrich/put] is unauthorized for user [test_enrich_no_privs]"));
    }
}
