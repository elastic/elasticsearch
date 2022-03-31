/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.test.rest.ESRestTestCase.entityAsMap;

public class TestSecurityClient {

    private final RestClient client;
    private final RequestOptions options;

    public TestSecurityClient(RestClient client) {
        this(client, RequestOptions.DEFAULT);
    }

    public TestSecurityClient(RestClient client, RequestOptions options) {
        this.client = client;
        this.options = options;
    }

    /**
     * Uses the REST API to retrieve the currently authenticated user.
     * @see User.Fields
     * @see org.elasticsearch.xpack.security.rest.action.RestAuthenticateAction
     */
    public Map<String, Object> authenticate() throws IOException {
        final String endpoint = "/_security/_authenticate";
        final Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        return entityAsMap(execute(request));
    }

    /**
     * Uses the REST API to create a new user in the native realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestPutUserAction
     */
    public void putUser(User user, SecureString password) throws IOException {
        final String endpoint = "/_security/user/" + user.principal();
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        final Map<String, Object> map = XContentTestUtils.convertToMap(user);
        if (password != null) {
            map.put("password", password.toString());
        }
        final String body = toJson(map);
        request.setJsonEntity(body);
        request.addParameters(Map.of("refresh", "true"));
        execute(request);
    }

    /**
     * Uses the REST API to delete a user from the native realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestDeleteUserAction
     */
    public void deleteUser(String username) throws IOException {
        final String endpoint = "/_security/user/" + username;
        final Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        request.addParameters(Map.of("refresh", "true"));
        execute(request);
    }

    /**
     * Uses the REST API to change the password of a user in the native/reserverd realms.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestChangePasswordAction
     */
    public void changePassword(String username, SecureString password) throws IOException {
        final String endpoint = "/_security/user/" + username + "/_password";
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        final String body = """
            {
                "password": "%s"
            }
            """.formatted(password.toString());
        request.setJsonEntity(body);
        execute(request);
    }

    /**
     * Uses the REST API to enable or disable a user in the native/reserved realm.
     * @see org.elasticsearch.xpack.security.rest.action.user.RestSetEnabledAction
     */
    public void setUserEnabled(String username, boolean enabled) throws IOException {
        final String endpoint = "/_security/user/" + username + "/" + (enabled ? "_enable" : "_disable");
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);
        execute(request);
    }

    /**
     * Uses the REST API to get a Role descriptor
     * @see org.elasticsearch.xpack.security.rest.action.role.RestGetRolesAction
     */
    public RoleDescriptor getRoleDescriptor(String roleName) throws IOException {
        if (Strings.isNullOrEmpty(roleName) || roleName.contains("*") || roleName.contains(",")) {
            throw new IllegalArgumentException("Provided role name must be for a single role (not [" + roleName + "])");
        }
        final Map<String, RoleDescriptor> descriptors = getRoleDescriptors(roleName);
        final RoleDescriptor descriptor = descriptors.get(roleName);
        if (descriptor == null) {
            throw new IllegalStateException("Did not find role [" + roleName + "]");
        }
        return descriptor;
    }

    public Map<String, RoleDescriptor> getRoleDescriptors(String[] roles) throws IOException {
        return getRoleDescriptors(Strings.arrayToCommaDelimitedString(roles));
    }

    private Map<String, RoleDescriptor> getRoleDescriptors(String roleParameter) throws IOException {
        final String endpoint = "/_security/role/" + roleParameter;
        final Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        final Response response = execute(request);
        final byte[] responseBody = EntityUtils.toByteArray(response.getEntity());
        final Map<String, RoleDescriptor> roles = new LinkedHashMap<>();
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, responseBody)) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                final String roleName = parser.currentName();
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                final RoleDescriptor role = RoleDescriptor.parse(roleName, parser, false);
                roles.put(roleName, role);
            }
        }
        return roles;
    }

    /**
     * Uses the REST API to create a new role in the native store.
     * @see org.elasticsearch.xpack.security.rest.action.role.RestPutRoleAction
     */
    public DocWriteResponse.Result putRole(RoleDescriptor descriptor) throws IOException {
        final String endpoint = "/_security/role/" + descriptor.getName();
        final Request request = new Request(HttpPut.METHOD_NAME, endpoint);

        final String body = toJson(descriptor);
        request.setJsonEntity(body);
        request.addParameters(Map.of("refresh", "true"));

        final Map<String, Object> response = entityAsMap(execute(request));

        final String createdFieldName = "role.created";
        final Object created = ObjectPath.eval(createdFieldName, response);

        if (Boolean.TRUE.equals(created)) {
            return DocWriteResponse.Result.CREATED;
        } else if (Boolean.FALSE.equals(created)) {
            return DocWriteResponse.Result.UPDATED;
        } else {
            throw new IllegalStateException(
                "Expected boolean for [" + createdFieldName + "] flag in [" + response + "], but was [" + created + "]"
            );
        }
    }

    /**
     * Uses the REST API to delete a role from the native store.
     * @see org.elasticsearch.xpack.security.rest.action.role.RestDeleteRoleAction
     */
    public boolean deleteRole(String roleName) throws IOException {
        final String endpoint = "/_security/role/" + roleName;
        final Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        final Map<String, Object> response = entityAsMap(execute(request));
        final Object found = response.get("found");
        if (found instanceof Boolean b) {
            return b;
        } else {
            throw new IllegalStateException("Expected boolean [found], but was [" + found + "]");
        }
    }

    /**
     * Uses the REST API to add a role-mapping to the native store.
     * @see org.elasticsearch.xpack.security.rest.action.rolemapping.RestPutRoleMappingAction
     */
    public void putRoleMapping(String mappingName, Map<String, Object> mappingBody) throws IOException {
        putRoleMapping(mappingName, toJson(mappingBody));
    }

    /**
     * Uses the REST API to add a role-mapping to the native store.
     * @see org.elasticsearch.xpack.security.rest.action.rolemapping.RestPutRoleMappingAction
     */
    public void putRoleMapping(String mappingName, String mappingJson) throws IOException {
        final String endpoint = "/_security/role_mapping/" + mappingName;
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setJsonEntity(mappingJson);
        execute(request);
    }

    /**
     * Uses the REST API to delete a role-mapping from the native store.
     * @see org.elasticsearch.xpack.security.rest.action.rolemapping.RestDeleteRoleMappingAction
     */
    public void deleteRoleMapping(String mappingName) throws IOException {
        final String endpoint = "/_security/role_mapping/" + mappingName;
        final Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        execute(request);
    }

    private static String toJson(Map<String, Object> map) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder().map(map);
        final BytesReference bytes = BytesReference.bytes(builder);
        return bytes.utf8ToString();
    }

    private static String toJson(ToXContent obj) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        if (obj.isFragment()) {
            builder.startObject();
            obj.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        } else {
            obj.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        final BytesReference bytes = BytesReference.bytes(builder);
        return bytes.utf8ToString();
    }

    private Response execute(Request request) throws IOException {
        request.setOptions(options);
        return this.client.performRequest(request);
    }

}
