/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rolemapping;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class RoleMappingRestIT extends ESRestTestCase {

    private static final String settingsJson = """
        {
             "metadata": {
                 "version": "1",
                 "compatibility": "8.4.0"
             },
             "state": {
                 "role_mappings": {
                       "role_mapping_1": {
                          "enabled": true,
                          "roles": [ "role_1" ],
                          "rules": { "field": { "username": "no_user" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                             "_foo": "something"
                          }
                       },
                       "role_mapping_2": {
                          "enabled": true,
                          "roles": [ "role_2" ],
                          "rules": { "field": { "username": "no_user" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be3-bb8d-02bb270cb3a7",
                             "_foo": "something_else"
                          }
                       }
                 }
             }
        }""";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .apply(SecurityOnTrialLicenseRestTestCase.commonTrialSecurityClusterConfig)
        .configFile("operator/settings.json", Resource.fromString(settingsJson))
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testGetRoleMappings() throws IOException {
        {
            final Map<String, Object> mappings = responseAsMap(client().performRequest(new Request("GET", "/_security/role_mapping")));
            assertThat(
                mappings.keySet(),
                containsInAnyOrder("role_mapping_1-read-only-operator-config", "role_mapping_2-read-only-operator-config")
            );
        }

        {
            final Map<String, Object> mappings = responseAsMap(
                client().performRequest(new Request("GET", "/_security/role_mapping/role_mapping_1-read-only-operator-config"))
            );
            assertThat(mappings.keySet(), containsInAnyOrder("role_mapping_1-read-only-operator-config"));
        }

        {
            final Map<String, Object> mappings = responseAsMap(
                client().performRequest(
                    new Request("GET", "/_security/role_mapping/role_mapping_1-read-only-operator-config,role_mapping_1")
                )
            );
            assertThat(mappings.keySet(), containsInAnyOrder("role_mapping_1-read-only-operator-config"));
        }
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

}
