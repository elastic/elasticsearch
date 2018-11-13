/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleAction;
import org.elasticsearch.xpack.core.security.action.role.DeleteRoleRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleAction;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import javax.crypto.SecretKeyFactory;
import java.io.Closeable;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class ApiKeyService {

    private static final Logger logger = LogManager.getLogger(ApiKeyService.class);
    private static final String TYPE = "doc";
    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = new Setting<>(
        "xpack.security.authc.api_key_hashing.algorithm", "pbkdf2", Function.identity(), (v, s) -> {
        if (Hasher.getAvailableAlgoStoredHash().contains(v.toLowerCase(Locale.ROOT)) == false) {
            throw new IllegalArgumentException("Invalid algorithm: " + v + ". Valid values for password hashing are " +
                Hasher.getAvailableAlgoStoredHash().toString());
        } else if (v.regionMatches(true, 0, "pbkdf2", 0, "pbkdf2".length())) {
            try {
                SecretKeyFactory.getInstance("PBKDF2withHMACSHA512");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalArgumentException(
                    "Support for PBKDF2WithHMACSHA512 must be available in order to use any of the " +
                        "PBKDF2 algorithms for the [xpack.security.authc.api_key.hashing.algorithm] setting.", e);
            }
        }
    }, Setting.Property.NodeScope);

    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final Hasher hasher;
    private final boolean enabled;

    public ApiKeyService(Settings settings, Clock clock, Client client, SecurityIndexManager securityIndex,
                         ClusterService clusterService) {
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.enabled = XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.get(settings);
        this.hasher = Hasher.resolve(PASSWORD_HASHING_ALGORITHM.get(settings));
    }

    /**
     * Asynchronously creates a new API key based off of the request and authentication
     * @param authentication the authentication that this api key should be based off of
     * @param request the request to create the api key included any permission restrictions
     * @param listener the listener that will be used to notify of completion
     */
    public void createApiKey(Authentication authentication, CreateApiKeyRequest request, ActionListener<CreateApiKeyResponse> listener) {
        ensureEnabled();
        if (authentication == null) {
            listener.onFailure(new IllegalArgumentException("authentication must be provided"));
        } else {
            final Instant created = clock.instant();
            final Instant expiration = getApiKeyExpiration(created, request);
            final SecureString apiKey = UUIDs.randomBase64UUIDSecureString();
            final Version version = clusterService.state().nodes().getMinNodeVersion();
            if (version.before(Version.V_7_0_0)) { // TODO(jaymode) change to V6_6_0 on backport!
                logger.warn("nodes prior to the minimum supported version for api keys {} exist in the cluster; these nodes will not be " +
                    "able to use api keys", Version.V_7_0_0);
            }

            final String id = UUIDs.base64UUID();
            final String roleName = "_es_apikey_role_" + id;
            final PutRoleRequest putRoleRequest = mergeRoleDescriptors(roleName, request.getRoleDescriptors());
            final char[] keyHash = hasher.hash(apiKey);
            IndexRequest indexRequest;
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                builder.startObject()
                    .field("doc_type", "api_key")
                    .field("creation_time", created.toEpochMilli())
                    .field("expiration_time", expiration == null ? null : expiration.toEpochMilli());

                byte[] utf8Bytes = null;
                try {
                    utf8Bytes = CharArrays.toUtf8Bytes(keyHash);
                    builder.field("api_key_hash").utf8Value(utf8Bytes, 0, utf8Bytes.length);
                } finally {
                    if (utf8Bytes != null) {
                        Arrays.fill(utf8Bytes, (byte) 0);
                    }
                }

                builder.array("roles", roleName)
                    .field("name", request.getName())
                    .field("version", version.id)
                    .startObject("creator")
                    .field("principal", authentication.getUser().principal())
                    .field("metadata", authentication.getUser().metadata())
                    .field("realm", authentication.getLookedUpBy() == null ?
                        authentication.getAuthenticatedBy().getName() : authentication.getLookedUpBy().getName())
                    .endObject()
                    .endObject();
                indexRequest = client.prepareIndex(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE, id)
                        .setOpType(DocWriteRequest.OpType.CREATE)
                        .setSource(builder)
                        .setRefreshPolicy(request.getRefreshPolicy())
                        .request();
            } catch (IOException e) {
                listener.onFailure(e);
                return;
            } finally {
                Arrays.fill(keyHash, (char) 0);
            }

            if (indexRequest == null) {
                throw new IllegalStateException("index request cannot be null");
            }

            client.execute(PutRoleAction.INSTANCE, putRoleRequest, ActionListener.wrap(response ->
                executeAsyncWithOrigin(client, SECURITY_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(indexResponse ->
                    listener.onResponse(new CreateApiKeyResponse(request.getName(), indexResponse.getId(), apiKey, expiration)),
                    e -> {
                        final DeleteRoleRequest deleteRoleRequest = new DeleteRoleRequest();
                        deleteRoleRequest.name(roleName);
                        client.execute(DeleteRoleAction.INSTANCE, deleteRoleRequest, ActionListener.wrap(deleteRoleResponse -> {
                            if (deleteRoleResponse.found()) {
                                logger.info("api key creation failed, but successfully cleaned up role: " + roleName);
                            } else {
                                logger.info("api key creation failed and no role found with name: " + roleName);
                            }
                        }, deleteEx -> logger.error(
                            new ParameterizedMessage("failed to clean up role [{}] after api key creation failure", roleName), deleteEx)));
                        listener.onFailure(e);
                    })),
                listener::onFailure));
        }
    }

    /**
     * Combines the provided role descriptors into a put role request with the specified name
     */
    private PutRoleRequest mergeRoleDescriptors(String name, List<RoleDescriptor> descriptors) {
        final List<RoleDescriptor.IndicesPrivileges> indicesPrivileges = new ArrayList<>();
        final Map<String, Object> metadata = new HashMap<>();
        final List<String> clusterPrivileges = new ArrayList<>();
        final List<String> runAsPrivileges = new ArrayList<>();
        final List<RoleDescriptor.ApplicationResourcePrivileges> applicationResourcePrivileges = new ArrayList<>();
        final List<ConditionalClusterPrivilege> conditionalClusterPrivileges = new ArrayList<>();
        for (RoleDescriptor descriptor : descriptors) {
            indicesPrivileges.addAll(Arrays.asList(descriptor.getIndicesPrivileges()));
            metadata.putAll(descriptor.getMetadata().entrySet().stream()
                .filter(e -> e.getKey().startsWith(MetadataUtils.RESERVED_PREFIX) == false)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            clusterPrivileges.addAll(Arrays.asList(descriptor.getClusterPrivileges()));
            runAsPrivileges.addAll(Arrays.asList(descriptor.getRunAs()));
            applicationResourcePrivileges.addAll(Arrays.asList(descriptor.getApplicationPrivileges()));
            conditionalClusterPrivileges.addAll(Arrays.asList(descriptor.getConditionalClusterPrivileges()));
        }

        final PutRoleRequest request = new PutRoleRequest();
        request.name(name);
        request.cluster(clusterPrivileges.toArray(Strings.EMPTY_ARRAY));
        request.addIndicesPrivileges(indicesPrivileges.toArray(new RoleDescriptor.IndicesPrivileges[0]));
        request.addApplicationPrivileges(applicationResourcePrivileges.toArray(new RoleDescriptor.ApplicationResourcePrivileges[0]));
        request.conditionalClusterPrivileges(conditionalClusterPrivileges.toArray(new ConditionalClusterPrivilege[0]));
        request.runAs(runAsPrivileges.toArray(Strings.EMPTY_ARRAY));
        request.metadata(metadata);
        return request;
    }

    /**
     * Checks for the presence of a {@code Authorization} header with a value that starts with
     * {@code ApiKey }. If found this will attempt to authenticate the key.
     */
    void authenticateWithApiKeyIfPresent(ThreadContext ctx, ActionListener<AuthenticationResult> listener) {
        if (enabled) {
            final ApiKeyCredentials credentials;
            try {
                credentials = getCredentialsFromHeader(ctx);
            } catch (IllegalArgumentException iae) {
                listener.onResponse(AuthenticationResult.unsuccessful(iae.getMessage(), iae));
                return;
            }

            if (credentials != null) {
                final GetRequest getRequest = client.prepareGet(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE, credentials.getId())
                    .setFetchSource(true).request();
                executeAsyncWithOrigin(ctx, SECURITY_ORIGIN, getRequest, ActionListener.<GetResponse>wrap(response -> {
                    if (response.isExists()) {
                        try (ApiKeyCredentials ignore = credentials) {
                            validateApiKeyCredentials(response.getSource(), credentials, clock, listener);
                        }
                    } else {
                        credentials.close();
                        listener.onResponse(AuthenticationResult.unsuccessful("unable to authenticate", null));
                    }
                }, e -> {
                    credentials.close();
                    listener.onResponse(AuthenticationResult.unsuccessful("apikey auth encountered a failure", e));
                }), client::get);
            } else {
                listener.onResponse(AuthenticationResult.notHandled());
            }
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    /**
     * Validates the ApiKey using the source map
     * @param source the source map from a get of the ApiKey document
     * @param credentials the credentials provided by the user
     * @param listener the listener to notify after verification
     */
    static void validateApiKeyCredentials(Map<String, Object> source, ApiKeyCredentials credentials, Clock clock,
                                          ActionListener<AuthenticationResult> listener) {
        final String apiKeyHash = (String) source.get("api_key_hash");
        if (apiKeyHash == null) {
            throw new IllegalStateException("api key hash is missing");
        }
        final boolean verified = verifyKeyAgainstHash(apiKeyHash, credentials);

        if (verified) {
            final Long expirationEpochMilli = (Long) source.get("expiration_time");
            if (expirationEpochMilli == null || Instant.ofEpochMilli(expirationEpochMilli).isAfter(clock.instant())) {
                final String principal = Objects.requireNonNull((String) source.get("principal"));
                final Map<String, Object> metadata = (Map<String, Object>) source.get("metadata");
                final List<String> roles;
                final Object rolesObj = source.get("roles");
                if (rolesObj instanceof String) {
                    roles = Collections.singletonList((String) rolesObj);
                } else {
                    roles = (List<String>) rolesObj;
                }
                final User apiKeyUser = new User(principal, roles.toArray(Strings.EMPTY_ARRAY), null, null, metadata, true);
                listener.onResponse(AuthenticationResult.success(apiKeyUser));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("api key is expired", null));
            }
        } else {
            listener.onResponse(AuthenticationResult.unsuccessful("invalid credentials", null));
        }
    }

    /**
     * Gets the API Key from the <code>Authorization</code> header if the header begins with
     * <code>ApiKey </code>
     */
    static ApiKeyCredentials getCredentialsFromHeader(ThreadContext threadContext) {
        String header = threadContext.getHeader("Authorization");
        if (Strings.hasText(header) && header.regionMatches(true, 0, "ApiKey ", 0, "ApiKey ".length())
            && header.length() > "ApiKey ".length()) {
            final byte[] decodedApiKeyCredBytes = Base64.getDecoder().decode(header.substring("ApiKey ".length()));
            char[] apiKeyCredChars = null;
            try {
                apiKeyCredChars = CharArrays.utf8BytesToChars(decodedApiKeyCredBytes);
                int colonIndex = -1;
                for (int i = 0; i < apiKeyCredChars.length; i++) {
                    if (apiKeyCredChars[i] == ':') {
                        colonIndex = i;
                        break;
                    }
                }

                if (colonIndex < 1) {
                    throw new IllegalArgumentException("invalid ApiKey value");
                }
                return new ApiKeyCredentials(new String(Arrays.copyOfRange(apiKeyCredChars, 0, colonIndex)),
                    new SecureString(Arrays.copyOfRange(apiKeyCredChars, colonIndex + 1, apiKeyCredChars.length)));
            } finally {
                if (apiKeyCredChars != null) {
                    Arrays.fill(apiKeyCredChars, (char) 0);
                }
            }
        }
        return null;
    }

    private static boolean verifyKeyAgainstHash(String apiKeyHash, ApiKeyCredentials credentials) {
        final char[] apiKeyHashChars = apiKeyHash.toCharArray();
        try {
            Hasher hasher = Hasher.resolveFromHash(apiKeyHash.toCharArray());
            return hasher.verify(credentials.getKey(), apiKeyHashChars);
        } finally {
            Arrays.fill(apiKeyHashChars, (char) 0);
        }
    }

    private Instant getApiKeyExpiration(Instant now, CreateApiKeyRequest request) {
        if (request.getExpiration() != null) {
            return now.plusSeconds(request.getExpiration().getSeconds());
        } else {
            return null;
        }
    }

    private void ensureEnabled() {
        if (enabled == false) {
            throw new IllegalStateException("tokens are not enabled");
        }
    }

    // package private class for testing
    static final class ApiKeyCredentials implements Closeable {
        private final String id;
        private final SecureString key;

        ApiKeyCredentials(String id, SecureString key) {
            this.id = id;
            this.key = key;
        }

        String getId() {
            return id;
        }

        SecureString getKey() {
            return key;
        }

        @Override
        public void close() {
            key.close();
        }
    }
}
