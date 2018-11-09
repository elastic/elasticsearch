/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.SecurityIndexSearcherWrapper;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.permission.SubsetResult;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.crypto.SecretKeyFactory;

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
    private final AuthorizationService authorizationService;
    private final ScriptService scriptService;
    private final NamedXContentRegistry xContentRegistry;

    public ApiKeyService(Settings settings, Clock clock, Client client, SecurityIndexManager securityIndex, ClusterService clusterService,
            AuthorizationService authorizationService, ScriptService scriptService, NamedXContentRegistry xContentRegistry) {
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.enabled = XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.get(settings);
        this.hasher = Hasher.resolve(PASSWORD_HASHING_ALGORITHM.get(settings));
        this.authorizationService = authorizationService;
        this.scriptService = scriptService;
        this.xContentRegistry = xContentRegistry;
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
            if (version.before(Version.V_7_0_0_alpha1)) { // TODO(jaymode) change to V6_6_0 on backport!
                logger.warn("nodes prior to the minimum supported version for api keys {} exist in the cluster; these nodes will not be " +
                    "able to use api keys", Version.V_7_0_0_alpha1);
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

                final List<RoleDescriptor> roleDescriptors = checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(
                        request.getRoleDescriptors(), authentication);

                builder.array("role_descriptors", roleDescriptors)
                    .field("name", request.getName())
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

    // pkg-scope for testing
    List<RoleDescriptor> checkIfRoleIsASubsetAndModifyRoleDescriptorsIfRequiredToMakeItASubset(
            final List<RoleDescriptor> requestRoleDescriptors, final Authentication authentication) throws IOException {
        List<RoleDescriptor> roleDescriptors = requestRoleDescriptors;

        final PlainActionFuture<Role> thisRoleFuture = new PlainActionFuture<>();
        authorizationService.roles(roleDescriptors, thisRoleFuture);
        final Role thisRole = thisRoleFuture.actionGet();

        final PlainActionFuture<Role> otherRoleFuture = new PlainActionFuture<>();
        authorizationService.roles(authentication.getUser(), otherRoleFuture);
        final Role otherRole = otherRoleFuture.actionGet();

        final SubsetResult subsetResult = thisRole.isSubsetOf(otherRole);

        if (subsetResult.result() == SubsetResult.Result.NO) {
            throw new ElasticsearchSecurityException("role descriptors from the request are not subset of the authenticated user");
        } else if (subsetResult.result() == SubsetResult.Result.MAYBE) {
            // we can make this work, by combining DLS queries such that
            // the resultant role descriptors are subset.
            final PlainActionFuture<Set<RoleDescriptor>> userRoleDescriptorsListener = new PlainActionFuture<>();
            authorizationService.roleDescriptors(authentication.getUser(), userRoleDescriptorsListener);
            final List<RoleDescriptor> otherRoleDescriptors = new ArrayList<>(userRoleDescriptorsListener.actionGet());
            roleDescriptors = modifyRoleDescriptorsToMakeItASubset(roleDescriptors, otherRoleDescriptors, subsetResult,
                    authentication.getUser());
        }
        return roleDescriptors;
    }

    private List<RoleDescriptor> modifyRoleDescriptorsToMakeItASubset(final List<RoleDescriptor> childDescriptors,
            final List<RoleDescriptor> baseDescriptors, final SubsetResult result, final User user) throws IOException {
        final Map<Set<String>, BoolQueryBuilder> indexNamePatternsToBoolQueryBuilder = new HashMap<>();
        for (Set<String> indexNamePattern : result.setOfIndexNamesForCombiningDLSQueries()) {
            Automaton indexNamesAutomaton = Automatons.patterns(indexNamePattern);
            BoolQueryBuilder parentFilterQueryBuilder = QueryBuilders.boolQuery();
            // Now find the index name patterns from all base descriptors that
            // match and combine queries
            for (RoleDescriptor rdbase : baseDescriptors) {
                for (IndicesPrivileges indicesPriv : rdbase.getIndicesPrivileges()) {
                    if (Operations.subsetOf(indexNamesAutomaton, Automatons.patterns(indicesPriv.getIndices()))) {
                        final String templateResult = SecurityIndexSearcherWrapper.evaluateTemplate(indicesPriv.getQuery().utf8ToString(),
                                scriptService, user);
                        try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry,
                                LoggingDeprecationHandler.INSTANCE, templateResult)) {
                            parentFilterQueryBuilder.should(AbstractQueryBuilder.parseInnerQueryBuilder(parser));
                        }
                    }
                }
            }
            parentFilterQueryBuilder.minimumShouldMatch(1);
            BoolQueryBuilder finalBoolQueryBuilder = QueryBuilders.boolQuery();
            finalBoolQueryBuilder.filter(parentFilterQueryBuilder);
            finalBoolQueryBuilder.minimumShouldMatch(1);
            // Iterate on child role descriptors and combine queries if the
            // index name patterns match.
            for (RoleDescriptor childRD : childDescriptors) {
                for (IndicesPrivileges ip : childRD.getIndicesPrivileges()) {
                    if (Sets.newHashSet(ip.getIndices()).equals(indexNamePattern)) {
                        final String templateResult = SecurityIndexSearcherWrapper.evaluateTemplate(ip.getQuery().utf8ToString(),
                                scriptService, user);
                        try (XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry,
                                LoggingDeprecationHandler.INSTANCE, templateResult)) {
                            finalBoolQueryBuilder.should(AbstractQueryBuilder.parseInnerQueryBuilder(parser));
                        }
                    }
                }
            }
            indexNamePatternsToBoolQueryBuilder.put(indexNamePattern, finalBoolQueryBuilder);
        }

        final List<RoleDescriptor> newChildDescriptors = new ArrayList<>();
        for (RoleDescriptor childRD : childDescriptors) {
            final Set<IndicesPrivileges> updates = new HashSet<>();
            for (IndicesPrivileges indicesPriv : childRD.getIndicesPrivileges()) {
                Set<String> indices = Sets.newHashSet(indicesPriv.getIndices());
                if (indexNamePatternsToBoolQueryBuilder.get(indices) != null) {
                    BoolQueryBuilder boolQueryBuilder = indexNamePatternsToBoolQueryBuilder.get(indices);
                    XContentBuilder builder = XContentFactory.jsonBuilder();
                    boolQueryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    updates.add(IndicesPrivileges.builder()
                            .indices(indicesPriv.getIndices())
                            .privileges(indicesPriv.getPrivileges())
                            .grantedFields(indicesPriv.getGrantedFields())
                            .deniedFields(indicesPriv.getDeniedFields())
                            .query(new BytesArray(Strings.toString(builder)))
                            .build());
                } else {
                    updates.add(indicesPriv);
                }
            }
            final RoleDescriptor rd = new RoleDescriptor(childRD.getName(), childRD.getClusterPrivileges(),
                    updates.toArray(new IndicesPrivileges[0]), childRD.getApplicationPrivileges(),
                    childRD.getConditionalClusterPrivileges(), childRD.getRunAs(), childRD.getMetadata(), childRD.getTransientMetadata());
            newChildDescriptors.add(rd);
        }
        assert newChildDescriptors.size() == childDescriptors.size();
        return newChildDescriptors;
    }
}
