/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.dataset;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.esql.datasources.EsqlDataSourcesCapabilities;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class RestPutDatasetActionTests extends ESTestCase {

    /**
     * The handler advertises the capability the yaml suite gates on. Pinned here because the yaml side fails soft: a
     * capability that stopped being served makes {@code requires} skip the section rather than fail it, so nothing
     * else turns dropping it into a red build.
     */
    public void testDeclaredTypeVocabularyCapabilityIsAdvertised() {
        assertThat(
            new RestPutDatasetAction(Set.of()).supportedCapabilities(),
            hasItem(EsqlDataSourcesCapabilities.DATASET_TEXT_TYPE_NOT_DECLARABLE)
        );
    }

    /** Same reasoning for the capability gating the {@code _id} rejection pin. */
    public void testIdRejectionCapabilityIsAdvertised() {
        assertThat(
            new RestPutDatasetAction(Set.of()).supportedCapabilities(),
            hasItem(EsqlDataSourcesCapabilities.DATASET_ID_NOT_DECLARABLE)
        );
    }

    /** Same reasoning for the capability gating the description length rejection pin. */
    public void testDescriptionLengthLimitCapabilityIsAdvertised() {
        assertThat(
            new RestPutDatasetAction(Set.of()).supportedCapabilities(),
            hasItem(EsqlDataSourcesCapabilities.DATA_SOURCE_DESCRIPTION_LENGTH_LIMIT)
        );
    }

    public void testGetFilteredFieldsEmpty() {
        assertThat(new RestPutDatasetAction(Set.of()).getFilteredFields(), empty());
    }

    public void testGetFilteredFieldsMapsToSettingsPaths() {
        RestPutDatasetAction action = new RestPutDatasetAction(Set.of("access_key", "secret_key", "session_token"));
        assertThat(action.getFilteredFields(), containsInAnyOrder("settings.access_key", "settings.secret_key", "settings.session_token"));
    }

    public void testFilterPresignedResource() throws IOException {
        RestPutDatasetAction action = new RestPutDatasetAction(Set.of("secret_key"));
        final var body = """
            {
              "data_source": "archive",
              "resource": "https://example-bucket.s3.amazonaws.com/data/sales.csv?X-Amz-Signature=0f1e2d3c4b5a69788796a5b4c3d2e1f0",
              "settings": {
                "secret_key": "should-be-removed",
                "endpoint": "https://127.0.0.1:9000"
              }
            }
            """;
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(body), XContentType.JSON)
            .build();
        RestRequest filtered = action.getFilteredRequest(restRequest);
        assertToXContentEquivalent(new BytesArray("""
            {
              "data_source": "archive",
              "resource": "https://example-bucket.s3.amazonaws.com/data/sales.csv",
              "settings": {
                "endpoint": "https://127.0.0.1:9000"
              }
            }
            """), filtered.content(), XContentType.JSON);
    }

    /** Pins that resource redaction still runs when there are no secret setting names to drop. */
    public void testFilterPresignedResourceWithEmptySecretNames() throws IOException {
        RestPutDatasetAction action = new RestPutDatasetAction(Set.of());
        final var body = """
            {
              "data_source": "archive",
              "resource": "https://example-bucket.s3.amazonaws.com/data/sales.csv?X-Amz-Signature=0f1e2d3c4b5a69788796a5b4c3d2e1f0"
            }
            """;
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(body), XContentType.JSON)
            .build();
        assertToXContentEquivalent(new BytesArray("""
            {
              "data_source": "archive",
              "resource": "https://example-bucket.s3.amazonaws.com/data/sales.csv"
            }
            """), action.getFilteredRequest(restRequest).content(), XContentType.JSON);
    }

    public void testFilterBodyWithNoResource() throws IOException {
        RestPutDatasetAction action = new RestPutDatasetAction(Set.of());
        final BytesArray body = new BytesArray("""
            {
              "data_source": "archive",
              "description": "no resource field"
            }
            """);
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(body, XContentType.JSON).build();
        assertToXContentEquivalent(body, action.getFilteredRequest(restRequest).content(), XContentType.JSON);
    }

    public void testFilterNonStringResourceIsSafe() throws IOException {
        RestPutDatasetAction action = new RestPutDatasetAction(Set.of());
        final BytesArray body = new BytesArray("""
            {
              "data_source": "archive",
              "resource": 12345
            }
            """);
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(body, XContentType.JSON).build();
        assertToXContentEquivalent(body, action.getFilteredRequest(restRequest).content(), XContentType.JSON);
    }

    public void testFilterRequestWithNoBodyReturnsSameRequest() {
        RestPutDatasetAction action = new RestPutDatasetAction(Set.of("secret_key"));
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).build();
        assertThat(action.getFilteredRequest(restRequest), sameInstance(restRequest));
    }

    /**
     * Malformed JSON must not throw from {@code content()} (that would be HTTP 500 during audit), and must not
     * return raw bytes that still contain secrets (AuditUtil would log them as {@code Invalid Format: ...}).
     */
    public void testFilterMalformedBodyOmitsSecrets() {
        RestPutDatasetAction action = new RestPutDatasetAction(Set.of("secret_key"));
        final String secret = "s3cr3t-must-not-appear-in-audit";
        // Truncated JSON: valid prefix holding a secret, then cut off so filtering cannot parse the body.
        final BytesArray body = new BytesArray("{\"data_source\":\"archive\",\"settings\":{\"secret_key\":\"" + secret + "\"");
        FakeRestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(body, XContentType.JSON).build();
        RestRequest filtered = action.getFilteredRequest(restRequest);
        assertThat(BytesReference.toBytes(filtered.content()), equalTo(BytesReference.toBytes(BytesArray.EMPTY)));
        assertThat(filtered.content().utf8ToString(), not(containsString(secret)));
    }
}
