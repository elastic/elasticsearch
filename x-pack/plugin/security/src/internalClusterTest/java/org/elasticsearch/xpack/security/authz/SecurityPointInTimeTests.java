/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchContextId;
import org.elasticsearch.action.search.SearchContextIdForNode;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class SecurityPointInTimeTests extends SecurityIntegTestCase {

    public void testCannotOpenPointInTimeOnInaccessibleIndex() {
        createSecurityIndexWithWaitForActiveShards();
        final SecureString password = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        createReadUser("reader", "read-allowed", "allowed", password);
        createIndex("allowed", "denied");
        ensureGreen("allowed", "denied");

        ElasticsearchSecurityException denied = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("reader", password)))
                .execute(TransportOpenPointInTimeAction.TYPE, new OpenPointInTimeRequest("denied").keepAlive(TimeValue.timeValueMinutes(1)))
                .actionGet()
        );
        assertThat(denied.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(denied.getMessage(), containsString("action [indices:data/read/open_point_in_time] is unauthorized for user [reader]"));
        assertThat(denied.getMessage(), containsString("denied"));
    }

    public void testCannotSearchPointInTimeOnInaccessibleIndex() {
        createSecurityIndexWithWaitForActiveShards();
        final SecureString password = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        createReadUser("owner", "read-owned", "owned", password);
        createReadUser("outsider", "read-other", "other", password);
        createIndex("owned", "other");
        ensureGreen("owned", "other");
        prepareIndex("owned").setId("1").setSource("value", "owned-doc").setRefreshPolicy(IMMEDIATE).get();

        BytesReference pit = openPointInTime("owner", password, TimeValue.timeValueMinutes(2), "owned");
        try {
            ElasticsearchSecurityException denied = expectThrows(
                ElasticsearchSecurityException.class,
                client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("outsider", password)))
                    .prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pit))
            );
            assertThat(denied.status(), equalTo(RestStatus.FORBIDDEN));
            assertThat(denied.getMessage(), containsString("action [indices:data/read/search] is unauthorized for user [outsider]"));
            assertThat(denied.getMessage(), not(containsString("owned-doc")));
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet();
        }
    }

    public void testPointInTimeCanBeUsedByAnotherUser() {
        createSecurityIndexWithWaitForActiveShards();
        final SecureString password = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        new PutRoleRequestBuilder(client()).name("read-shared")
            .addIndices(new String[] { "shared" }, new String[] { "read" }, null, null, null, randomBoolean())
            .get();
        createUser("opener", "read-shared", password);
        createUser("other", "read-shared", password);
        createIndex("shared");
        ensureGreen("shared");
        prepareIndex("shared").setId("1").setSource("value", "shared-doc").setRefreshPolicy(IMMEDIATE).get();

        BytesReference pit = openPointInTime("opener", password, TimeValue.timeValueMinutes(2), "shared");
        try {
            assertHitCount(
                client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("other", password)))
                    .prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pit)),
                1
            );
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pit)).actionGet();
        }
    }

    public void testPointInTimeReaderMustMatchAuthorizedShard() {
        createSecurityIndexWithWaitForActiveShards();
        new PutRoleRequestBuilder(client()).name("pit_public")
            .addIndices(new String[] { "pit-public" }, new String[] { "read" }, null, null, null, randomBoolean())
            .get();
        new PutRoleRequestBuilder(client()).name("pit_private")
            .addIndices(new String[] { "pit-private" }, new String[] { "read" }, null, null, null, randomBoolean())
            .get();
        final SecureString password = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
        new PutUserRequestBuilder(client()).username("pit_public_user")
            .password(password, getFastStoredHashAlgoForTests())
            .roles("pit_public")
            .get();
        new PutUserRequestBuilder(client()).username("pit_public_reader")
            .password(password, getFastStoredHashAlgoForTests())
            .roles("pit_public")
            .get();
        new PutUserRequestBuilder(client()).username("pit_private_user")
            .password(password, getFastStoredHashAlgoForTests())
            .roles("pit_private")
            .get();

        final String dataNode = clusterService().state().nodes().getDataNodes().values().iterator().next().getName();
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", dataNode) // force all shards onto 1 node
            .build();
        assertAcked(indicesAdmin().prepareCreate("pit-public").setSettings(settings));
        assertAcked(indicesAdmin().prepareCreate("pit-private").setSettings(settings));
        ensureGreen("pit-public", "pit-private");
        prepareIndex("pit-public").setId("1").setSource("canary", "public-doc").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex("pit-private").setId("1").setSource("canary", "private-canary").setRefreshPolicy(IMMEDIATE).get();

        ElasticsearchSecurityException denied = expectThrows(
            ElasticsearchSecurityException.class,
            client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("pit_public_user", password)))
                .prepareSearch("pit-private")
        );
        assertThat(denied.status(), equalTo(RestStatus.FORBIDDEN));

        BytesReference publicPit = openPointInTime("pit_public_user", password, TimeValue.timeValueMinutes(2), "pit-public");
        BytesReference privatePit = openPointInTime("pit_private_user", password, TimeValue.timeValueMinutes(2), "pit-private");
        try {
            assertHitCount(
                client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("pit_public_reader", password)))
                    .prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(publicPit)),
                1
            );

            SearchContextId publicId = SearchContextId.decode(writableRegistry(), publicPit);
            SearchContextId privateId = SearchContextId.decode(writableRegistry(), privatePit);
            Map.Entry<ShardId, SearchContextIdForNode> publicEntry = publicId.shards().entrySet().iterator().next();
            Map.Entry<ShardId, SearchContextIdForNode> privateEntry = privateId.shards().entrySet().iterator().next();
            BytesReference forged = encodePointInTime(
                publicEntry.getKey(),
                new SearchContextIdForNode(
                    publicEntry.getValue().getClusterAlias(),
                    publicEntry.getValue().getNode(),
                    privateEntry.getValue().getSearchContextId()
                ),
                publicId.aliasFilter()
            );

            SearchPhaseExecutionException failure = expectThrows(
                SearchPhaseExecutionException.class,
                client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue("pit_public_user", password)))
                    .prepareSearch()
                    .setAllowPartialSearchResults(randomBoolean())
                    .setPointInTime(new PointInTimeBuilder(forged))
            );
            assertThat(failure, not(instanceOf(ElasticsearchSecurityException.class)));
            Throwable cause = ExceptionsHelper.unwrapCause(failure.shardFailures()[0].getCause());
            assertThat(cause, instanceOf(IllegalArgumentException.class));
            assertThat(cause.getMessage(), equalTo("point in time id is not valid"));
            assertThat(failure.toString(), not(containsString("pit-private")));
            assertThat(failure.toString(), not(containsString("private-canary")));
        } finally {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(publicPit)).actionGet();
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(privatePit)).actionGet();
        }
    }

    private void createReadUser(String username, String role, String index, SecureString password) {
        new PutRoleRequestBuilder(client()).name(role)
            .addIndices(new String[] { index }, new String[] { "read" }, null, null, null, randomBoolean())
            .get();
        createUser(username, role, password);
    }

    private void createUser(String username, String role, SecureString password) {
        new PutUserRequestBuilder(client()).username(username).password(password, getFastStoredHashAlgoForTests()).roles(role).get();
    }

    private static BytesReference openPointInTime(String userName, SecureString password, TimeValue keepAlive, String... indices) {
        OpenPointInTimeRequest request = new OpenPointInTimeRequest(indices).keepAlive(keepAlive);
        OpenPointInTimeResponse response = client().filterWithHeader(Map.of(BASIC_AUTH_HEADER, basicAuthHeaderValue(userName, password)))
            .execute(TransportOpenPointInTimeAction.TYPE, request)
            .actionGet();
        return response.getPointInTimeId();
    }

    private static BytesReference encodePointInTime(ShardId shardId, SearchContextIdForNode context, Map<String, AliasFilter> aliasFilter) {
        PitSearchPhaseResult result = new PitSearchPhaseResult(context.getSearchContextId());
        result.setSearchShardTarget(new SearchShardTarget(context.getNode(), shardId, context.getClusterAlias()));
        return SearchContextId.encode(List.of(result), aliasFilter, TransportVersion.current(), ShardSearchFailure.EMPTY_ARRAY);
    }

    private static final class PitSearchPhaseResult extends SearchPhaseResult {
        private PitSearchPhaseResult(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }

    @After
    public void cleanupSecurityIndex() {
        super.deleteSecurityIndex();
    }
}
