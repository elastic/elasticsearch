/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.apikey;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.QueryApiKeyResponse;

import java.time.Instant;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;

public class ApiKeySingleNodeTests extends SecuritySingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        builder.put(XPackSettings.API_KEY_SERVICE_ENABLED_SETTING.getKey(), true);
        return builder.build();
    }

    public void testQueryWithExpiredKeys() throws InterruptedException {
        final String id1 = client().execute(CreateApiKeyAction.INSTANCE,
                new CreateApiKeyRequest("expired-shortly", null, TimeValue.timeValueMillis(1), null))
            .actionGet()
            .getId();
        final String id2 = client().execute(CreateApiKeyAction.INSTANCE,
                new CreateApiKeyRequest("long-lived", null, TimeValue.timeValueDays(1), null))
            .actionGet()
            .getId();
        Thread.sleep(10); // just to be 100% sure that the 1st key is expired when we search for it

        final QueryApiKeyRequest queryApiKeyRequest = new QueryApiKeyRequest(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.idsQuery().addIds(id1, id2))
                .filter(QueryBuilders.rangeQuery("expiration").from(Instant.now().toEpochMilli())));
        final QueryApiKeyResponse queryApiKeyResponse = client().execute(QueryApiKeyAction.INSTANCE, queryApiKeyRequest).actionGet();
        assertThat(queryApiKeyResponse.getItems().length, equalTo(1));
        assertThat(queryApiKeyResponse.getItems()[0].getApiKey().getId(), equalTo(id2));
        assertThat(queryApiKeyResponse.getItems()[0].getApiKey().getName(), equalTo("long-lived"));
        assertThat(queryApiKeyResponse.getItems()[0].getSortValues(), emptyArray());
    }
}
