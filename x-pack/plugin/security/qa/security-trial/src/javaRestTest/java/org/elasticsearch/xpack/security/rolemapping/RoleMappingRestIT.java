/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rolemapping;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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

        expect404(() -> getMappings("role-mapping-4"));
        expect404(() -> getMappings("role-mapping-4-read-only-operator-mapping"));

        ExpressionRoleMapping nativeMapping1 = expressionRoleMapping("role-mapping-1");
        putMapping(nativeMapping1, createOrUpdateWarning(nativeMapping1.getName()));

        ExpressionRoleMapping nativeMapping4 = expressionRoleMapping("role-mapping-4");
        putMapping(nativeMapping4);

        expectMappings(List.of(clusterStateMapping1, clusterStateMapping2, clusterStateMapping3, nativeMapping1, nativeMapping4));
        expectMappings(List.of(clusterStateMapping1, nativeMapping1), "role-mapping-1");
        expectMappings(List.of(clusterStateMapping1, nativeMapping1), "role-mapping-1", clusterStateMapping1.getName());
        expectMappings(List.of(clusterStateMapping1), clusterStateMapping1.getName());
        expectMappings(List.of(nativeMapping4), "role-mapping-4");
        expectMappings(List.of(nativeMapping4), "role-mapping-4", "role-mapping-4-read-only-operator-mapping");
    }

    public void testPutAndDeleteRoleMappings() throws IOException {
        {
            var ex = expectThrows(
                ResponseException.class,
                () -> putMapping(expressionRoleMapping("role-mapping-1-read-only-operator-mapping"))
            );
            assertThat(
                ex.getMessage(),
                containsString(
                    "Invalid mapping name [role-mapping-1-read-only-operator-mapping]. "
                        + "[-read-only-operator-mapping] is not an allowed suffix"
                )
            );
        }

        // Also fails even if a CS role mapping with that name does not exist
        {
            var ex = expectThrows(
                ResponseException.class,
                () -> putMapping(expressionRoleMapping("role-mapping-4-read-only-operator-mapping"))
            );
            assertThat(
                ex.getMessage(),
                containsString(
                    "Invalid mapping name [role-mapping-4-read-only-operator-mapping]. "
                        + "[-read-only-operator-mapping] is not an allowed suffix"
                )
            );
        }

        assertOK(putMapping(expressionRoleMapping("role-mapping-1"), createOrUpdateWarning("role-mapping-1")));

        assertOK(deleteMapping("role-mapping-1", deletionWarning("role-mapping-1")));

        // 404 without warnings if no native mapping exists
        expect404(() -> deleteMapping("role-mapping-1"));
    }

    private static void expect404(ThrowingRunnable clientCall) {
        var ex = expectThrows(ResponseException.class, clientCall);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    private static Response putMapping(ExpressionRoleMapping roleMapping) throws IOException {
        return putMapping(roleMapping, null);
    }

    private static Response putMapping(ExpressionRoleMapping roleMapping, @Nullable String warning) throws IOException {
        Request request = new Request("PUT", "/_security/role_mapping/" + roleMapping.getName());
        XContentBuilder xContent = roleMapping.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS);
        request.setJsonEntity(BytesReference.bytes(xContent).utf8ToString());
        if (warning != null) {
            request.setOptions(
                RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> warnings.equals(List.of(warning)) == false).build()
            );
        }
        return client().performRequest(request);
    }

    private static Response deleteMapping(String name) throws IOException {
        return deleteMapping(name, null);
    }

    private static Response deleteMapping(String name, @Nullable String warning) throws IOException {
        Request request = new Request("DELETE", "/_security/role_mapping/" + name);
        if (warning != null) {
            request.setOptions(
                RequestOptions.DEFAULT.toBuilder().setWarningsHandler(warnings -> warnings.equals(List.of(warning)) == false).build()
            );
        }
        return client().performRequest(request);
    }

    private static ExpressionRoleMapping expressionRoleMapping(String name) {
        return new ExpressionRoleMapping(
            name,
            new FieldExpression("username", List.of(new FieldExpression.FieldValue(randomAlphaOfLength(10)))),
            List.of(randomAlphaOfLength(5)),
            null,
            Map.of(),
            true
        );
    }

    @SuppressWarnings("unchecked")
    private static void expectMappings(List<ExpressionRoleMapping> expectedMappings, String... requestedMappingNames) throws IOException {
        Map<String, Object> map = responseAsMap(getMappings(requestedMappingNames));
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

    private static Response getMappings(String... requestedMappingNames) throws IOException {
        return client().performRequest(new Request("GET", "/_security/role_mapping/" + String.join(",", requestedMappingNames)));
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

    private static String createOrUpdateWarning(String mappingName) {
        return "A read-only role mapping with the same name ["
            + mappingName
            + "] has been previously defined in a configuration file. "
            + "Both role mappings will be used to determine role assignments.";
    }

    private static String deletionWarning(String mappingName) {
        return "A read-only role mapping with the same name ["
            + mappingName
            + "] has previously been defined in a configuration file. "
            + "The native role mapping was deleted, but the read-only mapping will remain active "
            + "and will be used to determine role assignments.";
    };
}
