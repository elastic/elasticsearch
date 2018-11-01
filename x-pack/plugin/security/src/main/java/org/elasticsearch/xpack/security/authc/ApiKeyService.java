/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class ApiKeyService {

    private static final Logger logger = LogManager.getLogger(ApiKeyService.class);
    private static final String TYPE = "doc";

    private final Clock clock;
    private final Client client;
    private final SecurityIndexManager securityIndex;
    private final ClusterService clusterService;
    private final boolean enabled;

    public ApiKeyService(Settings settings, Clock clock, Client client,
                         SecurityIndexManager securityIndex, ClusterService clusterService) {
        this.clock = clock;
        this.client = client;
        this.securityIndex = securityIndex;
        this.clusterService = clusterService;
        this.enabled = XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.get(settings);
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

            final char[] keyHash = Hasher.PBKDF2.hash(apiKey);
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

                builder.array("role_descriptors", request.getRoleDescriptors())
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
                        .setOpType(DocWriteRequest.OpType.CREATE)
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
}
