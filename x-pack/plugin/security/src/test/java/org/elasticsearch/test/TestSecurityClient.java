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
import org.apache.mina.core.RuntimeIoException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    /**
     * Uses the REST API to create a new access token via a password grant
     * @see org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction
     */
    public OAuth2Token createToken(UsernamePasswordToken grant) throws IOException {
        return createToken("""
            {
              "grant_type":"password",
              "username":"%s",
              "password":"%s"
            }
            """.formatted(grant.principal(), grant.credentials()));
    }

    /**
     * Uses the REST API to create a new access token via a refresh_token grant
     * @see org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction
     */
    public OAuth2Token refreshToken(String refreshToken) throws IOException {
        return createToken("""
            {
              "grant_type":"refresh_token",
              "refresh_token":"%s"
            }
            """.formatted(refreshToken));
    }

    /**
     * Uses the REST API to create a new access token via a client_credentials grant
     * @see org.elasticsearch.xpack.security.rest.action.oauth2.RestGetTokenAction
     */
    public OAuth2Token createTokenWithClientCredentialsGrant() throws IOException {
        return createToken("""
            {
              "grant_type":"client_credentials"
            }
            """);
    }

    private OAuth2Token createToken(String requestBody) throws IOException {
        final String endpoint = "/_security/oauth2/token";
        final Request request = new Request(HttpPost.METHOD_NAME, endpoint);
        request.setJsonEntity(requestBody);
        final Map<String, Object> responseBody = entityAsMap(execute(request));
        return new OAuth2Token(
            (String) responseBody.get("access_token"),
            Optional.ofNullable((String) responseBody.get("refresh_token")),
            ObjectPath.eval("authentication.username", responseBody)
        );
    }

    /**
     * Uses the REST API to invalidate an access-token
     * @see org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction
     */
    public TokenInvalidation invalidateAccessToken(String accessToken) throws IOException {
        return invalidateTokens("""
            {
              "token":"%s"
            }
            """.formatted(accessToken));
    }

    /**
     * Uses the REST API to invalidate a refresh-token
     * @see org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction
     */
    public TokenInvalidation invalidateRefreshToken(String refreshToken) throws IOException {
        return invalidateTokens("""
            {
              "refresh_token":"%s"
            }
            """.formatted(refreshToken));
    }

    /**
     * Uses the REST API to invalidate all tokens owned by a named user
     * @see org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction
     */
    public TokenInvalidation invalidateTokensForUser(String username) throws IOException {
        return invalidateTokens("""
            {
              "username":"%s"
            }
            """.formatted(username));
    }

    /**
     * Uses the REST API to invalidate all tokens owned by a named realm
     * @see org.elasticsearch.xpack.security.rest.action.oauth2.RestInvalidateTokenAction
     */
    public TokenInvalidation invalidateTokensForRealm(String realmName) throws IOException {
        return invalidateTokens("""
            {
              "realm_name":"%s"
            }
            """.formatted(realmName));
    }

    @SuppressWarnings("unchecked")
    private TokenInvalidation invalidateTokens(String requestBody) throws IOException {
        final String endpoint = "/_security/oauth2/token";
        final Request request = new Request(HttpDelete.METHOD_NAME, endpoint);
        // This API returns 404 (with the same body as a 200 response) if there's nothing to delete.
        // RestClient will throw an exception on 404, but we don't want that, we want to parse the body and return it
        request.addParameter("ignore", "404");
        request.setJsonEntity(requestBody);
        final Map<String, Object> responseBody = entityAsMap(execute(request));
        final List<Map<String, ?>> errors = (List<Map<String, ?>>) responseBody.get("error_details");
        return new TokenInvalidation(
            ((Number) responseBody.get("invalidated_tokens")).intValue(),
            ((Number) responseBody.get("previously_invalidated_tokens")).intValue(),
            errors == null ? List.of() : errors.stream().map(this::toException).toList()
        );
    }

    private static String toJson(Map<String, ? extends Object> map) throws IOException {
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

    private ElasticsearchException toException(Map<String, ?> map) {
        try {
            return ElasticsearchException.fromXContent(
                XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, toJson(map))
            );
        } catch (IOException e) {
            throw new RuntimeIoException(e);
        }
    }

    private Response execute(Request request) throws IOException {
        request.setOptions(options);
        return this.client.performRequest(request);
    }

    public record OAuth2Token(String accessToken, Optional<String> refreshToken, String principal) {

        @Nullable
        public String getRefreshToken() {
            return refreshToken.orElse(null);
        }
    }

    public record TokenInvalidation(int invalidated, int previouslyInvalidated, List<ElasticsearchException> errors) {}
}
