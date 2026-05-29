/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TokensPerFieldLimitIT extends ESIntegTestCase {

    public void testFieldWithinTokenLimit() {
        String indexName = "token-limit-ok";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexSettings.MAX_FIELD_TOKEN_COUNT_SETTING.getKey(), 100))
                .setMapping("body", "type=text")
        );

        // "hello world" produces 2 tokens with standard analyzer, well within 100
        DocWriteResponse response = prepareIndex(indexName).setSource("body", "hello world").get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    public void testFieldExceedingTokenLimit() {
        String indexName = "token-limit-exceeded";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexSettings.MAX_FIELD_TOKEN_COUNT_SETTING.getKey(), 3))
                .setMapping("body", "type=text")
        );

        // "one two three four five" produces 5 tokens, exceeds limit of 3
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.add(new IndexRequest(indexName).source("body", "one two three four five"));
        BulkResponse bulkResponse = bulkRequest.get();
        assertTrue(bulkResponse.hasFailures());
        BulkItemResponse itemResponse = bulkResponse.getItems()[0];
        assertTrue(itemResponse.isFailed());
        assertThat(itemResponse.getFailureMessage(), containsString("field [body]"));
        assertThat(itemResponse.getFailureMessage(), containsString("tokens_per_field.limit"));
        assertThat(itemResponse.getFailureMessage(), containsString("[3]"));
    }

    public void testTokenLimitDynamicUpdate() {
        String indexName = "token-limit-dynamic";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexSettings.MAX_FIELD_TOKEN_COUNT_SETTING.getKey(), 1000))
                .setMapping("body", "type=text")
        );

        // First, index a document within the limit
        DocWriteResponse response = prepareIndex(indexName).setSource("body", "hello world").get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        // Dynamically reduce the limit
        updateIndexSettings(Settings.builder().put(IndexSettings.MAX_FIELD_TOKEN_COUNT_SETTING.getKey(), 1), indexName);

        // Now the same document should fail
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.add(new IndexRequest(indexName).source("body", "hello world"));
        BulkResponse bulkResponse = bulkRequest.get();
        assertTrue(bulkResponse.hasFailures());
        assertThat(bulkResponse.getItems()[0].getFailureMessage(), containsString("tokens_per_field.limit"));
    }

    public void testBulkPartialFailure() {
        String indexName = "token-limit-bulk-partial";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexSettings.MAX_FIELD_TOKEN_COUNT_SETTING.getKey(), 3))
                .setMapping("body", "type=text")
        );

        BulkRequestBuilder bulkRequest = client().prepareBulk();
        // Doc 0: within limit (2 tokens)
        bulkRequest.add(new IndexRequest(indexName).id("ok1").source("body", "hello world"));
        // Doc 1: exceeds limit (5 tokens)
        bulkRequest.add(new IndexRequest(indexName).id("fail1").source("body", "one two three four five"));
        // Doc 2: within limit (1 token)
        bulkRequest.add(new IndexRequest(indexName).id("ok2").source("body", "single"));

        BulkResponse bulkResponse = bulkRequest.get();
        assertTrue(bulkResponse.hasFailures());

        BulkItemResponse[] items = bulkResponse.getItems();
        assertThat(items.length, equalTo(3));

        // Doc 0 should succeed
        assertFalse(items[0].isFailed());
        assertThat(items[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));

        // Doc 1 should fail
        assertTrue(items[1].isFailed());
        assertThat(items[1].getFailureMessage(), containsString("tokens_per_field.limit"));

        // Doc 2 should succeed
        assertFalse(items[2].isFailed());
        assertThat(items[2].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    public void testMultiValueFieldCountsPerValue() {
        String indexName = "token-limit-multi-value";
        assertAcked(
            indicesAdmin().prepareCreate(indexName)
                .setSettings(Settings.builder().put(IndexSettings.MAX_FIELD_TOKEN_COUNT_SETTING.getKey(), 4))
                .setMapping("body", "type=text")
        );

        // Multi-value field: each value is analyzed separately, so the limit applies per value.
        // ["one two three", "four five six"] = 3 tokens per value, both within limit of 4.
        DocWriteResponse response = prepareIndex(indexName).setSource("body", new String[] { "one two three", "four five six" }).get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));

        // A single value exceeding the limit should still fail
        BulkRequestBuilder bulkRequest = client().prepareBulk();
        bulkRequest.add(new IndexRequest(indexName).source("body", new String[] { "ok", "one two three four five" }));
        BulkResponse bulkResponse = bulkRequest.get();
        assertTrue(bulkResponse.hasFailures());
        assertThat(bulkResponse.getItems()[0].getFailureMessage(), containsString("tokens_per_field.limit"));
    }
}
