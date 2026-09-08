/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.BulkGrantApiKeyRequest;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RestBulkGrantApiKeyActionTests extends ESTestCase {

    public void testParseXContentForBulkGrantApiKeyRequest() throws Exception {
        final String grantType = randomAlphaOfLength(8);
        final String username = randomAlphaOfLength(8);
        final String password = randomAlphaOfLength(8);
        final String apiKeyName1 = randomAlphaOfLength(8);
        final String apiKeyName2 = randomAlphaOfLength(8);
        final var apiKeyExpiration = randomTimeValue();
        try (
            XContentParser content = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("grant_type", grantType)
                    .field("username", username)
                    .field("password", password)
                    .startArray("api_keys")
                    .startObject()
                    .field("name", apiKeyName1)
                    .field("expiration", apiKeyExpiration.getStringRep())
                    .endObject()
                    .startObject()
                    .field("name", apiKeyName2)
                    .endObject()
                    .endArray()
                    .endObject()
            )
        ) {
            BulkGrantApiKeyRequest request = RestBulkGrantApiKeyAction.RequestTranslator.Default.fromXContent(content);
            assertThat(request.getGrant().getType(), is(grantType));
            assertThat(request.getGrant().getUsername(), is(username));
            assertThat(request.getGrant().getPassword(), is(new SecureString(password.toCharArray())));
            assertThat(request.getApiKeyRequests().size(), equalTo(2));
            assertThat(request.getApiKeyRequests().get(0).getName(), is(apiKeyName1));
            assertThat(request.getApiKeyRequests().get(0).getExpiration(), is(apiKeyExpiration));
            assertThat(request.getApiKeyRequests().get(1).getName(), is(apiKeyName2));
        }
    }
}
