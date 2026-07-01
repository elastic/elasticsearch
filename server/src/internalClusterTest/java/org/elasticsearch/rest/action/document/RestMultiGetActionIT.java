/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.InputStreamReader;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * REST-level coverage for {@code _mget} with the slice-indexing {@code _slice} parameter.
 * On a slice-enabled index, an mget item must address its
 * doc via {@code _slice} (the routing value); the parameter is parsed both per-item and as a top-level default, validated
 * per-item against the item's own index, and routed by slice so the lookup reaches the correct shard.
 */
public class RestMultiGetActionIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    private void createSliceIndex(String index, boolean sliceEnabled) throws Exception {
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {
                "index.slice.enabled": %s,
                "index.number_of_shards": 2
              }
            }""", sliceEnabled));
        getRestClient().performRequest(create);
    }

    private void seedDoc(String index, String id, String slice) throws Exception {
        Request seed = new Request("POST", "/" + index + "/_doc/" + id);
        if (slice != null) {
            seed.addParameter("_slice", slice);
        }
        seed.addParameter("refresh", "true");
        seed.setJsonEntity(Strings.format("""
            {
              "field": "value-%s"
            }""", id));
        getRestClient().performRequest(seed);
    }

    public void testMgetRetrievesBySliceAndRequiresIt() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-mget-it", true);
        // Distinct ids in distinct slices: with two shards, the slice (routing) decides the shard, so a correct mget must
        // route each item by its _slice to find the doc.
        seedDoc("slice-mget-it", "1", "s1");
        seedDoc("slice-mget-it", "2", "s2");

        Request mget = new Request("POST", "/slice-mget-it/_mget");
        mget.setJsonEntity("""
            {
              "docs": [
                { "_id": "1", "_slice": "s1" },
                { "_id": "2", "_slice": "s2" }
              ]
            }""");
        ObjectPath found = ObjectPath.createFromResponse(getRestClient().performRequest(mget));
        assertThat(found.evaluate("docs.0.found"), equalTo(Boolean.TRUE));
        assertThat(found.evaluate("docs.0._source.field"), equalTo("value-1"));
        assertThat(found.evaluate("docs.1.found"), equalTo(Boolean.TRUE));
        assertThat(found.evaluate("docs.1._source.field"), equalTo("value-2"));

        // An item that omits _slice on a slice-enabled index fails individually (mget still returns 200 for the batch).
        Request missingSlice = new Request("POST", "/slice-mget-it/_mget");
        missingSlice.setJsonEntity("""
            {
              "docs": [
                { "_id": "1" }
              ]
            }""");
        ObjectPath missing = ObjectPath.createFromResponse(getRestClient().performRequest(missingSlice));
        assertThat(missing.evaluate("docs.0.error.reason"), containsString("[_slice] is required when [index.slice.enabled] is true"));
    }

    public void testMgetTopLevelSliceDefaultAppliesToIds() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-mget-default-it", true);
        seedDoc("slice-mget-default-it", "1", "s1");

        // The top-level _slice acts as the per-item default, so the ids form (which has no place for a per-item _slice) works.
        Request mget = new Request("POST", "/slice-mget-default-it/_mget");
        mget.addParameter("_slice", "s1");
        mget.setJsonEntity("""
            {
              "ids": [ "1" ]
            }""");
        ObjectPath objectPath = ObjectPath.createFromResponse(getRestClient().performRequest(mget));
        assertThat(objectPath.evaluate("docs.0.found"), equalTo(Boolean.TRUE));
        assertThat(objectPath.evaluate("docs.0._source.field"), equalTo("value-1"));
    }

    public void testMgetSliceValidationErrors() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-mget-invalid-it", true);

        // routing together with _slice on the same item is rejected while parsing the request body.
        Request routingAndSlice = new Request("POST", "/slice-mget-invalid-it/_mget");
        routingAndSlice.setJsonEntity("""
            {
              "docs": [
                { "_id": "1", "routing": "s1", "_slice": "s1" }
              ]
            }""");
        ResponseException routingAndSliceException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(routingAndSlice)
        );
        assertThat(bodyOf(routingAndSliceException), containsString("[routing] is not allowed together with [_slice]"));

        // The reserved _all value is not a valid write-side slice.
        Request reservedSlice = new Request("POST", "/slice-mget-invalid-it/_mget");
        reservedSlice.addParameter("_slice", "_all");
        reservedSlice.setJsonEntity("""
            {
              "ids": [ "1" ]
            }""");
        ResponseException reservedSliceException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(reservedSlice)
        );
        assertThat(bodyOf(reservedSliceException), containsString("invalid [_slice] value [_all]"));
    }

    public void testMgetSliceRejectedWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-mget-disabled-it", false);
        seedDoc("slice-mget-disabled-it", "1", null);

        Request mget = new Request("POST", "/slice-mget-disabled-it/_mget");
        mget.setJsonEntity("""
            {
              "docs": [
                { "_id": "1", "_slice": "s1" }
              ]
            }""");
        ObjectPath objectPath = ObjectPath.createFromResponse(getRestClient().performRequest(mget));
        assertThat(
            objectPath.evaluate("docs.0.error.reason"),
            containsString("[_slice] is not allowed when [index.slice.enabled] is false")
        );
    }

    public void testMgetSliceParamRejectedWhenFeatureFlagDisabled() throws Exception {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request mget = new Request("POST", "/_mget");
        mget.addParameter("_slice", "s1");
        mget.setJsonEntity("""
            {
              "docs": [
                { "_index": "test_index", "_id": "1" }
              ]
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(mget));
        assertThat(bodyOf(exception), containsString("request does not support [_slice]"));
    }

    private static String bodyOf(ResponseException e) throws Exception {
        return Streams.copyToString(new InputStreamReader(e.getResponse().getEntity().getContent(), UTF_8));
    }
}
