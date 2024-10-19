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
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
                       "role-mapping-1": {
                          "enabled": true,
                          "roles": [ "role_1" ],
                          "rules": { "field": { "username": "no_user" } },
                          "metadata": {
                             "uuid" : "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7",
                             "_foo": "something",
                             "_es_reserved_role_mapping_name": "ignored"
                          }
                       },
                       "role-mapping-2": {
                          "enabled": true,
                          "roles": [ "role_2" ],
                          "rules": { "field": { "username": "no_user" } }
                       },
                       "role-mapping-3": {
                          "enabled": true,
                          "roles": [ "role_3" ],
                          "rules": { "field": { "username": "no_user" } },
                          "metadata": {
                             "_read_only" : { "field": 1 },
                             "_es_reserved_role_mapping_name": { "still_ignored": true }
                          }
                       }
                 }
             }
        }""";
    private static final ExpressionRoleMapping clusterStateMapping1 = new ExpressionRoleMapping(
        "role-mapping-1-read-only-operator-mapping",
        new FieldExpression("username", List.of(new FieldExpression.FieldValue("no_user"))),
        List.of("role_1"),
        null,
        Map.of("uuid", "b9a59ba9-6b92-4be2-bb8d-02bb270cb3a7", "_foo", "something", "_read_only", true),
        true
    );
    private static final ExpressionRoleMapping clusterStateMapping2 = new ExpressionRoleMapping(
        "role-mapping-2-read-only-operator-mapping",
        new FieldExpression("username", List.of(new FieldExpression.FieldValue("no_user"))),
        List.of("role_2"),
        null,
        Map.of("_read_only", true),
        true
    );
    private static final ExpressionRoleMapping clusterStateMapping3 = new ExpressionRoleMapping(
        "role-mapping-3-read-only-operator-mapping",
        new FieldExpression("username", List.of(new FieldExpression.FieldValue("no_user"))),
        List.of("role_3"),
        null,
        Map.of("_read_only", true),
        true
    );

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
        expectMappings(List.of(clusterStateMapping1, clusterStateMapping2, clusterStateMapping3));
        expectMappings(List.of(clusterStateMapping1), "role-mapping-1");
        expectMappings(List.of(clusterStateMapping1, clusterStateMapping3), "role-mapping-1", "role-mapping-3");
        expectMappings(List.of(clusterStateMapping1), clusterStateMapping1.getName());
        expectMappings(List.of(clusterStateMapping1), clusterStateMapping1.getName(), "role-mapping-1");
    }

    @SuppressWarnings("unchecked")
    private static void expectMappings(List<ExpressionRoleMapping> expectedMappings, String... requestedMappingNames) throws IOException {
        Map<String, Object> map = responseAsMap(
            client().performRequest(new Request("GET", "/_security/role_mapping/" + String.join(",", requestedMappingNames)))
        );
        assertThat(
            map.keySet(),
            containsInAnyOrder(expectedMappings.stream().map(ExpressionRoleMapping::getName).toList().toArray(new String[0]))
        );
        List<ExpressionRoleMapping> actualMappings = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            XContentParser body = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, (Map<String, ?>) entry.getValue());
            ExpressionRoleMapping actual = ExpressionRoleMapping.parse(entry.getKey(), body);
            actualMappings.add(actual);
        }
        assertThat(actualMappings, containsInAnyOrder(expectedMappings.toArray(new ExpressionRoleMapping[0])));
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
