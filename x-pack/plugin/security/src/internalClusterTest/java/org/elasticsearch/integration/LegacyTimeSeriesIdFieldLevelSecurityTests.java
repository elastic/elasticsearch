/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;

import java.util.Collections;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;

public class LegacyTimeSeriesIdFieldLevelSecurityTests extends SecurityIntegTestCase {

    private static final SecureString USERS_PASSWD = SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected String configUsers() {
        final String usersPasswHashed = new String(getFastStoredHashAlgoForTests().hash(USERS_PASSWD));
        return super.configUsers() + "user_hidden_host:" + usersPasswHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            role_hidden_host:user_hidden_host
            """;
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + """
            role_hidden_host:
              cluster: [ none ]
              indices:
                - names: 'tsdb-*'
                  privileges: [ read ]
                  field_security:
                    grant: [ '*' ]
                    except: [ host ]
            """;
    }

    public void testSearchIdOfLegacyIndexWhenFieldLevelSecurityHidesRoutingPathField() {
        final String index = "tsdb-legacy";
        createTimeSeriesIndex(index, IndexVersions.UPGRADE_TO_LUCENE_9_10);
        final String indexedId = indexDocument(index);

        assertSearchedId(client(), index, indexedId);
        assertSearchedId(clientOf("user_hidden_host"), index, indexedId);
    }

    public void testSearchIdOfCurrentIndexWhenFieldLevelSecurityHidesRoutingPathField() {
        final String index = "tsdb-current";
        createTimeSeriesIndex(index, IndexVersion.current());
        final String indexedId = indexDocument(index);

        assertSearchedId(client(), index, indexedId);
        assertSearchedId(clientOf("user_hidden_host"), index, indexedId);
    }

    public void testGetByIdReturnedBySearchOfLegacyIndexWhenFieldLevelSecurityHidesRoutingPathField() {
        final String index = "tsdb-legacy";
        createTimeSeriesIndex(index, IndexVersions.UPGRADE_TO_LUCENE_9_10);
        indexDocument(index);

        assertGetByIdReturnedBySearch(clientOf("user_hidden_host"), index);
    }

    public void testGetByIdReturnedBySearchOfCurrentIndexWhenFieldLevelSecurityHidesRoutingPathField() {
        final String index = "tsdb-current";
        createTimeSeriesIndex(index, IndexVersion.current());
        indexDocument(index);

        assertGetByIdReturnedBySearch(clientOf("user_hidden_host"), index);
    }

    private void createTimeSeriesIndex(String index, IndexVersion indexVersionCreated) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersionCreated)
                        .put(IndexSettings.MODE.getKey(), "time_series")
                        .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host", "region")
                        .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2026-01-01T00:00:00Z")
                        .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2026-01-02T00:00:00Z")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "region",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=double"
                )
        );
    }

    private String indexDocument(String index) {
        return prepareIndex(index).setSource("@timestamp", "2026-01-01T00:00:01Z", "host", "h1", "region", "eu", "cpu", 0.5)
            .setRefreshPolicy(IMMEDIATE)
            .get()
            .getId();
    }

    private Client clientOf(String user) {
        return client().filterWithHeader(Collections.singletonMap(BASIC_AUTH_HEADER, basicAuthHeaderValue(user, USERS_PASSWD)));
    }

    private static void assertGetByIdReturnedBySearch(Client client, String index) {
        final SearchResponse searchResponse = client.prepareSearch(index).get();
        try {
            assertThat(searchResponse.getHits().getHits().length, equalTo(1));
            final String returnedId = searchResponse.getHits().getHits()[0].getId();
            assertThat("get by the _id returned by search", client.prepareGet(index, returnedId).get().isExists(), equalTo(true));
        } finally {
            searchResponse.decRef();
        }
    }

    private static void assertSearchedId(Client client, String index, String expectedId) {
        assertResponse(client.prepareSearch(index), response -> {
            assertThat(response.getHits().getHits().length, equalTo(1));
            assertThat(response.getHits().getHits()[0].getId(), equalTo(expectedId));
        });
    }
}
