/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.GetApiKeyResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomCrossClusterAccessRoleDescriptor;
import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTestHelper.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class RestGetApiKeyActionTests extends ESTestCase {
    private final XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);
    private Settings settings = null;
    private ThreadPool threadPool = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder()
            .put("path.home", createTempDir().toString())
            .put("node.name", "test-" + getTestName())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
            .build();
        threadPool = new ThreadPool(settings, MeterRegistry.NOOP, new DefaultBuiltInExecutorBuilders());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testGetApiKey() throws Exception {
        final Map<String, String> param1 = Map.of("realm_name", "realm-1", "username", "user-x");
        final Map<String, String> param2 = Map.of("realm_name", "realm-1");
        final Map<String, String> param3 = Map.of("username", "user-x");
        final Map<String, String> param4 = Map.of("id", "api-key-id-1");
        final Map<String, String> param5 = Map.of("name", "api-key-name-1");
        final Map<String, String> params = new HashMap<>(randomFrom(param1, param2, param3, param4, param5));
        final boolean withLimitedBy = randomBoolean();
        if (withLimitedBy) {
            params.put("with_limited_by", "true");
        } else {
            if (randomBoolean()) {
                params.put("with_limited_by", "false");
            }
        }
        final boolean replyEmptyResponse = rarely();
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(params).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };
        final List<String> profileUids = randomSize1ProfileUidsList();
        final ApiKey apiKey = randomApiKeyInfo(withLimitedBy);
        final GetApiKeyResponse getApiKeyResponseExpected = new GetApiKeyResponse(List.of(apiKey), profileUids);

        final var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                ActionRequestValidationException validationException = getApiKeyRequest.validate();
                if (validationException != null) {
                    listener.onFailure(validationException);
                    return;
                }
                if (getApiKeyRequest.getApiKeyName() != null && getApiKeyRequest.getApiKeyName().equals("api-key-name-1")
                    || getApiKeyRequest.getApiKeyId() != null && getApiKeyRequest.getApiKeyId().equals("api-key-id-1")
                    || getApiKeyRequest.getRealmName() != null && getApiKeyRequest.getRealmName().equals("realm-1")
                    || getApiKeyRequest.getUserName() != null && getApiKeyRequest.getUserName().equals("user-x")) {
                    if (replyEmptyResponse) {
                        listener.onResponse((Response) GetApiKeyResponse.EMPTY);
                    } else {
                        listener.onResponse((Response) getApiKeyResponseExpected);
                    }
                } else {
                    listener.onFailure(new ElasticsearchSecurityException("encountered an error while creating API key"));
                }
            }
        };
        final RestGetApiKeyAction restGetApiKeyAction = new RestGetApiKeyAction(Settings.EMPTY, mockLicenseState);

        restGetApiKeyAction.handleRequest(restRequest, restChannel, client);

        final RestResponse restResponse = responseSetOnce.get();
        assertNotNull(restResponse);
        assertThat(restResponse.status(), (replyEmptyResponse && params.get("id") != null) ? is(RestStatus.NOT_FOUND) : is(RestStatus.OK));
        final GetApiKeyResponse actual = GetApiKeyResponse.fromXContent(createParser(XContentType.JSON.xContent(), restResponse.content()));
        if (replyEmptyResponse) {
            assertThat(actual.getApiKeyInfoList(), emptyIterable());
        } else {
            assertThat(
                actual.getApiKeyInfoList(),
                contains(new GetApiKeyResponse.Item(apiKey, profileUids == null ? null : profileUids.get(0)))
            );
        }
    }

    public void testGetApiKeyWithProfileUid() throws Exception {
        final boolean isGetRequestWithProfileUid = randomBoolean();
        final Map<String, String> param = new HashMap<>();
        if (isGetRequestWithProfileUid) {
            param.put("with_profile_uid", Boolean.TRUE.toString());
        } else {
            if (randomBoolean()) {
                param.put("with_profile_uid", Boolean.FALSE.toString());
            }
        }
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(param).build();
        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };
        final ApiKey apiKey1 = randomApiKeyInfo(randomBoolean());
        final List<String> profileUids1 = randomSize1ProfileUidsList();
        final var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                ActionRequestValidationException validationException = getApiKeyRequest.validate();
                if (validationException != null) {
                    listener.onFailure(validationException);
                    return;
                }

                if (getApiKeyRequest.withProfileUid()) {
                    listener.onResponse((Response) new GetApiKeyResponse(List.of(apiKey1), profileUids1));
                } else {
                    listener.onResponse((Response) new GetApiKeyResponse(List.of(apiKey1), null));
                }
            }
        };
        final RestGetApiKeyAction restGetApiKeyAction = new RestGetApiKeyAction(Settings.EMPTY, mockLicenseState);
        restGetApiKeyAction.handleRequest(restRequest, restChannel, client);
        final RestResponse restResponse = responseSetOnce.get();
        assertNotNull(restResponse);
        assertThat(restResponse.status(), is(RestStatus.OK));
        final GetApiKeyResponse actual = GetApiKeyResponse.fromXContent(createParser(XContentType.JSON.xContent(), restResponse.content()));
        boolean responseHasProfile = isGetRequestWithProfileUid && profileUids1 != null && profileUids1.get(0) != null;
        if (responseHasProfile) {
            assertThat(actual.getApiKeyInfoList(), contains(new GetApiKeyResponse.Item(apiKey1, profileUids1.get(0))));
        } else {
            assertThat(actual.getApiKeyInfoList(), contains(new GetApiKeyResponse.Item(apiKey1, null)));
        }
    }

    public void testGetApiKeyOwnedByCurrentAuthenticatedUser() throws Exception {
        final boolean isGetRequestForOwnedKeysOnly = randomBoolean();
        final Map<String, String> param = new HashMap<>();
        if (isGetRequestForOwnedKeysOnly) {
            param.put("owner", Boolean.TRUE.toString());
        } else {
            param.put("owner", Boolean.FALSE.toString());
            param.put("realm_name", "realm-1");
        }
        final boolean withLimitedBy = randomBoolean();
        if (withLimitedBy) {
            param.put("with_limited_by", "true");
        } else {
            if (randomBoolean()) {
                param.put("with_limited_by", "false");
            }
        }

        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(param).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final ApiKey apiKey1 = randomApiKeyInfo(withLimitedBy);
        final List<String> profileUids1 = randomSize1ProfileUidsList();
        final ApiKey apiKey2 = randomApiKeyInfo(withLimitedBy);
        final List<String> profileUids2 = randomSize2ProfileUidsList();
        final GetApiKeyResponse getApiKeyResponseExpectedWhenOwnerFlagIsTrue = new GetApiKeyResponse(List.of(apiKey1), profileUids1);
        final GetApiKeyResponse getApiKeyResponseExpectedWhenOwnerFlagIsFalse = new GetApiKeyResponse(
            List.of(apiKey1, apiKey2),
            profileUids2
        );

        final var client = new NodeClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                GetApiKeyRequest getApiKeyRequest = (GetApiKeyRequest) request;
                ActionRequestValidationException validationException = getApiKeyRequest.validate();
                if (validationException != null) {
                    listener.onFailure(validationException);
                    return;
                }

                if (getApiKeyRequest.ownedByAuthenticatedUser()) {
                    listener.onResponse((Response) getApiKeyResponseExpectedWhenOwnerFlagIsTrue);
                } else if (getApiKeyRequest.getRealmName() != null && getApiKeyRequest.getRealmName().equals("realm-1")) {
                    listener.onResponse((Response) getApiKeyResponseExpectedWhenOwnerFlagIsFalse);
                }
            }
        };
        final RestGetApiKeyAction restGetApiKeyAction = new RestGetApiKeyAction(Settings.EMPTY, mockLicenseState);

        restGetApiKeyAction.handleRequest(restRequest, restChannel, client);

        final RestResponse restResponse = responseSetOnce.get();
        assertNotNull(restResponse);
        assertThat(restResponse.status(), is(RestStatus.OK));
        final GetApiKeyResponse actual = GetApiKeyResponse.fromXContent(createParser(XContentType.JSON.xContent(), restResponse.content()));
        if (isGetRequestForOwnedKeysOnly) {
            assertThat(
                actual.getApiKeyInfoList(),
                contains(new GetApiKeyResponse.Item(apiKey1, profileUids1 == null ? null : profileUids1.get(0)))
            );
        } else {
            assertThat(
                actual.getApiKeyInfoList(),
                contains(
                    new GetApiKeyResponse.Item(apiKey1, profileUids2 == null ? null : profileUids2.get(0)),
                    new GetApiKeyResponse.Item(apiKey2, profileUids2 == null ? null : profileUids2.get(1))
                )
            );
        }
    }

    private static List<String> randomSize1ProfileUidsList() {
        final List<String> profileUids;
        if (randomBoolean()) {
            if (randomBoolean()) {
                profileUids = null;
            } else {
                profileUids = new ArrayList<>(1);
                profileUids.add(null);
            }
        } else {
            profileUids = new ArrayList<>(1);
            profileUids.add(randomAlphaOfLength(8));
        }
        return profileUids;
    }

    private static List<String> randomSize2ProfileUidsList() {
        final List<String> profileUids2;
        if (randomBoolean()) {
            if (randomBoolean()) {
                profileUids2 = null;
            } else {
                profileUids2 = new ArrayList<>(2);
                profileUids2.add(null);
                profileUids2.add(null);
            }
        } else {
            profileUids2 = new ArrayList<>(2);
            if (randomBoolean()) {
                if (randomBoolean()) {
                    profileUids2.add(randomAlphaOfLength(8));
                    profileUids2.add(null);
                } else {
                    profileUids2.add(null);
                    profileUids2.add(randomAlphaOfLength(8));
                }
            } else {
                profileUids2.add(randomAlphaOfLength(8));
                profileUids2.add(randomAlphaOfLength(8));
            }
        }
        return profileUids2;
    }

    private ApiKey randomApiKeyInfo(boolean withLimitedBy) {
        final ApiKey.Type type = randomFrom(ApiKey.Type.values());
        final Instant creation = Instant.now();
        final Instant expiration = randomFrom(Arrays.asList(null, Instant.now().plus(10, ChronoUnit.DAYS)));
        final Map<String, Object> metadata = ApiKeyTests.randomMetadata();
        final List<RoleDescriptor> roleDescriptors = type == ApiKey.Type.CROSS_CLUSTER
            ? List.of(randomCrossClusterAccessRoleDescriptor())
            : randomUniquelyNamedRoleDescriptors(0, 3);
        final List<RoleDescriptor> limitedByRoleDescriptors = withLimitedBy && type != ApiKey.Type.CROSS_CLUSTER
            ? randomUniquelyNamedRoleDescriptors(1, 3)
            : null;
        return new ApiKey(
            "api-key-name-" + randomAlphaOfLength(4),
            "api-key-id-" + randomAlphaOfLength(4),
            type,
            creation,
            expiration,
            false,
            null,
            "user-x",
            "realm-1",
            "realm-type-1",
            metadata,
            roleDescriptors,
            limitedByRoleDescriptors
        );
    }
}
