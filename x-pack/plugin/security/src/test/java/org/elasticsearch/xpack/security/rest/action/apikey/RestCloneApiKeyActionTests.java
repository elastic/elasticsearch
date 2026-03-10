/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.security.action.apikey.CloneApiKeyRequest;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RestCloneApiKeyActionTests extends ESTestCase {

    private String randomBase64ApiKey() {
        return Base64.getEncoder().encodeToString((randomUUID() + ":" + randomUUID()).getBytes(StandardCharsets.UTF_8));
    }

    public void testParseXContentWithRequiredFields() throws Exception {
        final String encodedApiKey = randomBase64ApiKey();
        final String name = randomAlphaOfLength(8);
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder().startObject().field("api_key", encodedApiKey).field("name", name).endObject()
            )
        ) {
            CloneApiKeyRequest request = RestCloneApiKeyAction.PARSER.parse(parser, null);
            assertThat(request.getApiKey(), notNullValue());
            assertThat(request.getApiKey().toString(), is(encodedApiKey));
            assertThat(request.getName(), is(name));
            assertThat(request.getExpiration(), nullValue());
            assertThat(request.getMetadata(), nullValue());
        }
    }

    public void testParseXContentWithExpirationValue() throws Exception {
        final String encodedApiKey = randomBase64ApiKey();
        final String name = randomAlphaOfLength(8);
        final String expiration = "30d";
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("api_key", encodedApiKey)
                    .field("name", name)
                    .field("expiration", expiration)
                    .endObject()
            )
        ) {
            CloneApiKeyRequest request = RestCloneApiKeyAction.PARSER.parse(parser, null);
            assertThat(request.getExpiration(), notNullValue());
            assertThat(request.getExpiration().getStringRep(), is(expiration));
        }
    }

    public void testParseXContentWithExpirationNull() throws Exception {
        final String encodedApiKey = randomBase64ApiKey();
        final String name = randomAlphaOfLength(8);
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("api_key", encodedApiKey)
                    .field("name", name)
                    .nullField("expiration")
                    .endObject()
            )
        ) {
            CloneApiKeyRequest request = RestCloneApiKeyAction.PARSER.parse(parser, null);
            assertThat(request.getExpiration(), equalTo(TimeValue.MINUS_ONE));
        }
    }

    public void testParseXContentWithMetadata() throws Exception {
        final String encodedApiKey = randomBase64ApiKey();
        final String name = randomAlphaOfLength(8);
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("api_key", encodedApiKey)
                    .field("name", name)
                    .startObject("metadata")
                    .field("foo", "bar")
                    .endObject()
                    .endObject()
            )
        ) {
            CloneApiKeyRequest request = RestCloneApiKeyAction.PARSER.parse(parser, null);
            assertThat(request.getMetadata(), notNullValue());
            assertThat(request.getMetadata(), is(Map.of("foo", "bar")));
        }
    }

    public void testFilteredFieldsIncludeApiKey() {
        RestCloneApiKeyAction action = new RestCloneApiKeyAction(Settings.EMPTY, new XPackLicenseState(() -> 0));
        Set<String> filtered = action.getFilteredFields();
        assertThat(filtered, hasItem("api_key"));
    }

    public void testValidateRejectsClonedFromInMetadata() throws Exception {
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("api_key", randomBase64ApiKey())
                    .field("name", "clone")
                    .startObject("metadata")
                    .field(ApiKeyService.CLONED_FROM_RESERVED_METADATA_KEY, "source-id")
                    .endObject()
                    .endObject()
            )
        ) {
            CloneApiKeyRequest request = RestCloneApiKeyAction.PARSER.parse(parser, null);
            request.setRefreshPolicy(org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE);
            var validation = request.validate();
            assertThat(validation, notNullValue());
            assertThat(validation.validationErrors().get(0), containsString(MetadataUtils.RESERVED_PREFIX));
        }
    }
}
