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
import static org.hamcrest.Matchers.notNullValue;

/**
 * REST-level coverage for {@code _termvectors} and {@code _mtermvectors} with the slice-indexing {@code _slice} parameter.
 * On a slice-enabled index a term vectors lookup must address its doc via {@code _slice} (the routing value): the parameter
 * is parsed from the URI (single, and the multi default) and per-item from the {@code _mtermvectors} body, validated against
 * the target index, and routed by slice so the lookup reaches the shard holding that slice's copy of the id.
 */
public class RestTermVectorsActionIT extends ESIntegTestCase {

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
              },
              "mappings": {
                "properties": {
                  "field": { "type": "text", "term_vector": "with_positions_offsets" }
                }
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
              "field": "the quick brown fox %s"
            }""", id));
        getRestClient().performRequest(seed);
    }

    public void testTermVectorsRetrievesBySliceAndRequiresIt() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-tv-it", true);
        // Same id in distinct slices: with two shards the slice (routing) decides the shard, so the lookup must route by _slice.
        seedDoc("slice-tv-it", "1", "s1");
        seedDoc("slice-tv-it", "1", "s2");

        // The id resolves within the requested slice...
        Request inSlice = new Request("GET", "/slice-tv-it/_termvectors/1");
        inSlice.addParameter("_slice", "s1");
        ObjectPath found = ObjectPath.createFromResponse(getRestClient().performRequest(inSlice));
        assertThat(found.evaluate("found"), equalTo(Boolean.TRUE));
        assertThat(found.evaluate("_id"), equalTo("1"));
        assertThat(found.evaluate("term_vectors.field"), notNullValue());

        // ...and a slice that has no copy of the id reports not-found rather than another slice's doc.
        Request otherSlice = new Request("GET", "/slice-tv-it/_termvectors/2");
        otherSlice.addParameter("_slice", "s1");
        ObjectPath notFound = ObjectPath.createFromResponse(getRestClient().performRequest(otherSlice));
        assertThat(notFound.evaluate("found"), equalTo(Boolean.FALSE));

        // Omitting _slice on a slice-enabled index is rejected at the coordinating node (a hard 400 for the single API).
        Request missingSlice = new Request("GET", "/slice-tv-it/_termvectors/1");
        ResponseException missing = expectThrows(ResponseException.class, () -> getRestClient().performRequest(missingSlice));
        assertThat(bodyOf(missing), containsString("[_slice] is required when [index.slice.enabled] is true"));

        // Raw routing is rejected in favor of _slice.
        Request rawRouting = new Request("GET", "/slice-tv-it/_termvectors/1");
        rawRouting.addParameter("routing", "s1");
        ResponseException routing = expectThrows(ResponseException.class, () -> getRestClient().performRequest(rawRouting));
        assertThat(bodyOf(routing), containsString("[routing] is not allowed when [index.slice.enabled] is true"));
    }

    public void testMtermvectorsRetrievesBySliceAndRequiresIt() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-mtv-it", true);
        seedDoc("slice-mtv-it", "1", "s1");
        seedDoc("slice-mtv-it", "2", "s2");

        Request mtv = new Request("POST", "/slice-mtv-it/_mtermvectors");
        mtv.setJsonEntity("""
            {
              "docs": [
                { "_id": "1", "_slice": "s1" },
                { "_id": "2", "_slice": "s2" }
              ]
            }""");
        ObjectPath found = ObjectPath.createFromResponse(getRestClient().performRequest(mtv));
        assertThat(found.evaluate("docs.0.found"), equalTo(Boolean.TRUE));
        assertThat(found.evaluate("docs.0._id"), equalTo("1"));
        assertThat(found.evaluate("docs.1.found"), equalTo(Boolean.TRUE));
        assertThat(found.evaluate("docs.1._id"), equalTo("2"));

        // An item that omits _slice fails individually; the batch still returns 200.
        Request missingSlice = new Request("POST", "/slice-mtv-it/_mtermvectors");
        missingSlice.setJsonEntity("""
            {
              "docs": [
                { "_id": "1" }
              ]
            }""");
        ObjectPath missing = ObjectPath.createFromResponse(getRestClient().performRequest(missingSlice));
        assertThat(missing.evaluate("docs.0.error.reason"), containsString("[_slice] is required when [index.slice.enabled] is true"));
    }

    public void testMtermvectorsTopLevelSliceDefaultAppliesToIds() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-mtv-default-it", true);
        seedDoc("slice-mtv-default-it", "1", "s1");

        // The top-level _slice acts as the per-item default, so the ids form (which has no place for a per-item _slice) works.
        Request mtv = new Request("POST", "/slice-mtv-default-it/_mtermvectors");
        mtv.addParameter("_slice", "s1");
        mtv.setJsonEntity("""
            {
              "ids": [ "1" ]
            }""");
        ObjectPath objectPath = ObjectPath.createFromResponse(getRestClient().performRequest(mtv));
        assertThat(objectPath.evaluate("docs.0.found"), equalTo(Boolean.TRUE));
        assertThat(objectPath.evaluate("docs.0._id"), equalTo("1"));
    }

    public void testTermVectorsSliceValidationErrors() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-tv-invalid-it", true);

        // routing together with _slice on the same item is rejected while parsing the request body.
        Request routingAndSlice = new Request("POST", "/slice-tv-invalid-it/_mtermvectors");
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
        Request reservedSlice = new Request("GET", "/slice-tv-invalid-it/_termvectors/1");
        reservedSlice.addParameter("_slice", "_all");
        ResponseException reservedSliceException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(reservedSlice)
        );
        assertThat(bodyOf(reservedSliceException), containsString("invalid [_slice] value [_all]"));
    }

    public void testTermVectorsSliceRejectedWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        createSliceIndex("slice-tv-disabled-it", false);
        seedDoc("slice-tv-disabled-it", "1", null);

        // Single API: a hard 400 at the coordinating node.
        Request single = new Request("GET", "/slice-tv-disabled-it/_termvectors/1");
        single.addParameter("_slice", "s1");
        ResponseException singleException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(single));
        assertThat(bodyOf(singleException), containsString("[_slice] is not allowed when [index.slice.enabled] is false"));

        // Multi API: an individual item failure within a 200 batch.
        Request multi = new Request("POST", "/slice-tv-disabled-it/_mtermvectors");
        multi.setJsonEntity("""
            {
              "docs": [
                { "_id": "1", "_slice": "s1" }
              ]
            }""");
        ObjectPath objectPath = ObjectPath.createFromResponse(getRestClient().performRequest(multi));
        assertThat(
            objectPath.evaluate("docs.0.error.reason"),
            containsString("[_slice] is not allowed when [index.slice.enabled] is false")
        );
    }

    public void testTermVectorsSliceParamRejectedWhenFeatureFlagDisabled() throws Exception {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request single = new Request("GET", "/test_index/_termvectors/1");
        single.addParameter("_slice", "s1");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(single));
        assertThat(bodyOf(exception), containsString("request does not support [_slice]"));
    }

    private static String bodyOf(ResponseException e) throws Exception {
        return Streams.copyToString(new InputStreamReader(e.getResponse().getEntity().getContent(), UTF_8));
    }
}
