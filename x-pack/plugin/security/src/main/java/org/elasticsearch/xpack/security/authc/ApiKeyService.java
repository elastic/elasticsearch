/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.ScopedRole;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.crypto.SecretKeyFactory;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class ApiKeyService {

    private static final Logger logger = LogManager.getLogger(ApiKeyService.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    private static final String TYPE = "doc";
    static final String API_KEY_ID_KEY = "_security_api_key_id";
    static final String API_KEY_ROLE_DESCRIPTORS_KEY = "_security_api_key_role_descriptors";
    static final String API_KEY_SCOPED_ROLE_DESCRIPTORS_KEY = "_security_api_key_scoped_role_descriptors";
    static final String API_KEY_ROLE_KEY = "_security_api_key_role";

    public static final Setting<String> PASSWORD_HASHING_ALGORITHM = new Setting<>(
        "xpack.security.authc.api_key_hashing.algorithm", "pbkdf2", Function.identity(), v -> {
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
    private final CompositeRolesStore compositeRolesStore;

    public ApiKeyService(Settings settings, Clock clock, Client client, SecurityIndexManager securityIndex, ClusterService clusterService,
            CompositeRolesStore compositeRolesStore) {
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.enabled = XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.get(settings);
        this.hasher = Hasher.resolve(PASSWORD_HASHING_ALGORITHM.get(settings));
        this.compositeRolesStore = compositeRolesStore;
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

            final char[] keyHash = hasher.hash(apiKey);
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

                // Save role_descriptors
                builder.startObject("role_descriptors");
                if (request.getRoleDescriptors() != null && request.getRoleDescriptors().isEmpty() == false) {
                    for (RoleDescriptor descriptor : request.getRoleDescriptors()) {
                        builder.field(descriptor.getName(),
                                (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
                    }
                } else {
                    builder.nullValue();
                }
                builder.endObject();

                // Save scoped_role_descriptors
                builder.startObject("scoped_role_descriptors");
                compositeRolesStore.getRoleDescriptors(Sets.newHashSet(authentication.getUser().roles()), ActionListener.wrap(rdSet -> {
                    for (RoleDescriptor descriptor : rdSet) {
                        builder.field(descriptor.getName(),
                                (contentBuilder, params) -> descriptor.toXContent(contentBuilder, params, true));
                    }
                }, listener::onFailure));
                builder.endObject();

                builder.field("name", request.getName())
                    .field("version", version.id)
                    .startObject("creator")
                    .field("principal", authentication.getUser().principal())
                    .field("metadata", authentication.getUser().metadata())
                    .field("realm", authentication.getLookedUpBy() == null ?
                        authentication.getAuthenticatedBy().getName() : authentication.getLookedUpBy().getName())
                    .endObject()
                    .endObject();
                final IndexRequest indexRequest =
                    client.prepareIndex(SecurityIndexManager.SECURITY_INDEX_NAME, TYPE)
                        .setSource(builder)
                        .setRefreshPolicy(request.getRefreshPolicy())
                        .request();
                securityIndex.prepareIndexIfNeededThenExecute(listener::onFailure, () ->
                    executeAsyncWithOrigin(client, SECURITY_ORIGIN, IndexAction.INSTANCE, indexRequest,
                        ActionListener.wrap(indexResponse ->
                                listener.onResponse(new CreateApiKeyResponse(request.getName(), indexResponse.getId(), apiKey, expiration)),
                            listener::onFailure)));
            } catch (IOException e) {
                listener.onFailure(e);
            } finally {
                Arrays.fill(keyHash, (char) 0);
            }
        }
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
                            final Map<String, Object> source = response.getSource();
                            validateApiKeyCredentials(source, credentials, clock, listener);
                        }
                    } else {
                        credentials.close();
                        listener.onResponse(
                            AuthenticationResult.unsuccessful("unable to find apikey with id " + credentials.getId(), null));
                    }
                }, e -> {
                    credentials.close();
                    listener.onResponse(AuthenticationResult.unsuccessful("apikey authentication for id " + credentials.getId() +
                        " encountered a failure", e));
                }), client::get);
            } else {
                listener.onResponse(AuthenticationResult.notHandled());
            }
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    /**
     * The current request has been authenticated by an API key and this method enables the
     * retrieval of role descriptors that are associated with the api key and triggers the building
     * of the {@link Role} to authorize the request.
     */
    public void getRoleForApiKey(Authentication authentication, CompositeRolesStore rolesStore, ActionListener<Role> listener) {
        if (authentication.getAuthenticationType() != Authentication.AuthenticationType.API_KEY) {
            throw new IllegalStateException("authentication type must be api key but is " + authentication.getAuthenticationType());
        }

        final Map<String, Object> metadata = authentication.getMetadata();
        final String apiKeyId = (String) metadata.get(API_KEY_ID_KEY);

        final Map<String, Object> roleDescriptors = (Map<String, Object>) metadata.get(API_KEY_ROLE_DESCRIPTORS_KEY);
        final Map<String, Object> authnRoleDescriptors = (Map<String, Object>) metadata.get(API_KEY_SCOPED_ROLE_DESCRIPTORS_KEY);

        if (roleDescriptors == null && authnRoleDescriptors == null) {
            listener.onFailure(new ElasticsearchSecurityException("no role descriptors found for API key"));
        } else if (roleDescriptors == null) {
            final List<RoleDescriptor> authnRoleDescriptorsList = parseRoleDescriptors(apiKeyId, authnRoleDescriptors);
            rolesStore.buildAndCacheRoleFromDescriptors(authnRoleDescriptorsList, apiKeyId, listener);
        } else {
            final List<RoleDescriptor> roleDescriptorList = parseRoleDescriptors(apiKeyId, roleDescriptors);
            final List<RoleDescriptor> authnRoleDescriptorsList = parseRoleDescriptors(apiKeyId, authnRoleDescriptors);
            rolesStore.buildAndCacheRoleFromDescriptors(roleDescriptorList, apiKeyId, ActionListener.wrap(role -> {
                rolesStore.buildAndCacheRoleFromDescriptors(authnRoleDescriptorsList, apiKeyId, ActionListener.wrap(scopedByRole -> {
                    Role finalRole = ScopedRole.createScopedRole(role, scopedByRole);
                    listener.onResponse(finalRole);
                }, e -> listener.onFailure(e)));
            }, e -> listener.onFailure(e)));
        }

    }

    private List<RoleDescriptor> parseRoleDescriptors(final String apiKeyId, final Map<String, Object> roleDescriptors) {
        if (roleDescriptors == null) {
            return null;
        }
        return roleDescriptors.entrySet().stream()
            .map(entry -> {
                final String name = entry.getKey();
                final Map<String, Object> rdMap = (Map<String, Object>) entry.getValue();
                try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    builder.map(rdMap);
                    try (XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                        new ApiKeyLoggingDeprecationHandler(deprecationLogger, apiKeyId),
                        BytesReference.bytes(builder).streamInput())) {
                        return RoleDescriptor.parse(name, parser, false);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).collect(Collectors.toList());
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
                final Map<String, Object> creator = Objects.requireNonNull((Map<String, Object>) source.get("creator"));
                final String principal = Objects.requireNonNull((String) creator.get("principal"));
                final Map<String, Object> metadata = (Map<String, Object>) creator.get("metadata");
                final Map<String, Object> roleDescriptors = (Map<String, Object>) source.get("role_descriptors");
                final Map<String, Object> scopedRoleDescriptors = (Map<String, Object>) source
                        .get("scoped_role_descriptors");
                final String[] roleNames = (roleDescriptors != null) ? roleDescriptors.keySet().toArray(Strings.EMPTY_ARRAY)
                        : scopedRoleDescriptors.keySet().toArray(Strings.EMPTY_ARRAY);
                final User apiKeyUser = new User(principal, roleNames, null, null, metadata, true);
                final Map<String, Object> authResultMetadata = new HashMap<>();
                authResultMetadata.put(API_KEY_ROLE_DESCRIPTORS_KEY, roleDescriptors);
                authResultMetadata.put(API_KEY_SCOPED_ROLE_DESCRIPTORS_KEY, scopedRoleDescriptors);
                authResultMetadata.put(API_KEY_ID_KEY, credentials.getId());
                listener.onResponse(AuthenticationResult.success(apiKeyUser, authResultMetadata));
            } else {
                listener.onResponse(AuthenticationResult.terminate("api key is expired", null));
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
            throw new IllegalStateException("api keys are not enabled");
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

    private static class ApiKeyLoggingDeprecationHandler implements DeprecationHandler {

        private final DeprecationLogger deprecationLogger;
        private final String apiKeyId;

        private ApiKeyLoggingDeprecationHandler(DeprecationLogger logger, String apiKeyId) {
            this.deprecationLogger = logger;
            this.apiKeyId = apiKeyId;
        }

        @Override
        public void usedDeprecatedName(String usedName, String modernName) {
            deprecationLogger.deprecated("Deprecated field [{}] used in api key [{}], expected [{}] instead",
                usedName, apiKeyId, modernName);
        }

        @Override
        public void usedDeprecatedField(String usedName, String replacedWith) {
            deprecationLogger.deprecated("Deprecated field [{}] used in api key [{}], replaced by [{}]",
                usedName, apiKeyId, replacedWith);
        }
    }
}
