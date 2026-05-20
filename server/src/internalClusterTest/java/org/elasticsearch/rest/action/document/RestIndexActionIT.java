/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class RestIndexActionIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testIndexWithSourceOnErrorDisabled() throws Exception {
        var source = "{\"field\": \"value}";
        var sourceEscaped = "{\\\"field\\\": \\\"value}";

        var request = new Request("POST", "/test_index/_doc/1");
        request.setJsonEntity(source);

        var exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString(sourceEscaped));

        // disable source on error
        request.addParameter(RestUtils.INCLUDE_SOURCE_ON_ERROR_PARAMETER, "false");
        exception = assertThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(
            response,
            both(not(containsString(sourceEscaped))).and(
                containsString("REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled)")
            )
        );
    }

    public void testSliceValidationAndRequirement() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-index-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request missingSlice = new Request("POST", "/slice-index-it/_doc/1");
        missingSlice.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException missingSliceException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(missingSlice));
        String missingSliceBody = Streams.copyToString(
            new InputStreamReader(missingSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(missingSliceBody, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request invalidSlice = new Request("POST", "/slice-index-it/_doc/2");
        invalidSlice.addParameter(SliceIndexing.PARAM_NAME, "_all");
        invalidSlice.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException invalidSliceException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(invalidSlice));
        String invalidSliceBody = Streams.copyToString(
            new InputStreamReader(invalidSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(invalidSliceBody, containsString("invalid [_slice] value"));

        Request validSlice = new Request("POST", "/slice-index-it/_doc/3");
        validSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
        validSlice.setJsonEntity("""
            {
              "field": "value"
            }""");
        Response response = getRestClient().performRequest(validSlice);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("result"), equalTo("created"));
    }

    public void testSliceRejectedWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-index-disabled");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              }
            }""");
        getRestClient().performRequest(create);

        Request request = new Request("POST", "/slice-index-disabled/_doc/1");
        request.addParameter(SliceIndexing.PARAM_NAME, "s1");
        request.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }

    public void testRoutingRejectedWhenSliceEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-index-routing-rejected");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request request = new Request("POST", "/slice-index-routing-rejected/_doc/1");
        request.addParameter("routing", "r1");
        request.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[routing] is not allowed when [index.slice.enabled] is true"));
        assertThat(response, containsString("use [_slice] instead"));
    }

    public void testSliceParamRejectedWhenFeatureFlagDisabled() throws Exception {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request request = new Request("POST", "/test_index/_doc/1");
        request.addParameter(SliceIndexing.PARAM_NAME, "s1");
        request.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("request does not support [_slice]"));
    }

    public void testSliceFieldAliasWorksForQueries() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-query-filter-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request index1 = new Request("POST", "/slice-query-filter-it/_doc/1");
        index1.addParameter(SliceIndexing.PARAM_NAME, "s1");
        index1.setJsonEntity("""
            {
              "field": "a"
            }""");
        getRestClient().performRequest(index1);

        Request index2 = new Request("POST", "/slice-query-filter-it/_doc/2");
        index2.addParameter(SliceIndexing.PARAM_NAME, "s1");
        index2.setJsonEntity("""
            {
              "field": "b"
            }""");
        getRestClient().performRequest(index2);

        Request index3 = new Request("POST", "/slice-query-filter-it/_doc/3");
        index3.addParameter(SliceIndexing.PARAM_NAME, "s2");
        index3.setJsonEntity("""
            {
              "field": "c"
            }""");
        getRestClient().performRequest(index3);

        getRestClient().performRequest(new Request("POST", "/slice-query-filter-it/_refresh"));

        Request filterBySlice = new Request("GET", "/slice-query-filter-it/_search");
        filterBySlice.addParameter(SliceIndexing.PARAM_NAME, SliceIndexing.SLICE_ALL);
        filterBySlice.setJsonEntity("""
            {
              "size": 0,
              "query": {
                "term": {
                  "_slice": "s1"
                }
              }
            }""");
        Response filterResponse = getRestClient().performRequest(filterBySlice);
        ObjectPath filterResponsePath = ObjectPath.createFromResponse(filterResponse);
        assertThat(filterResponsePath.evaluate("hits.total.value"), equalTo(2));

        Request prefixBySlice = new Request("GET", "/slice-query-filter-it/_search");
        prefixBySlice.addParameter(SliceIndexing.PARAM_NAME, SliceIndexing.SLICE_ALL);
        prefixBySlice.setJsonEntity("""
            {
              "size": 0,
              "query": {
                "prefix": {
                  "_slice": "s1"
                }
              }
            }""");
        Response prefixResponse = getRestClient().performRequest(prefixBySlice);
        ObjectPath prefixResponsePath = ObjectPath.createFromResponse(prefixResponse);
        assertThat(prefixResponsePath.evaluate("hits.total.value"), equalTo(2));
    }

    public void testSliceQueryBehavesAsUnmappedWhenSliceDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-query-disabled-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              }
            }""");
        getRestClient().performRequest(create);

        Request index = new Request("POST", "/slice-query-disabled-it/_doc/1");
        index.addParameter("routing", "s1");
        index.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(index);
        getRestClient().performRequest(new Request("POST", "/slice-query-disabled-it/_refresh"));

        Request termBySlice = new Request("GET", "/slice-query-disabled-it/_search");
        termBySlice.setJsonEntity("""
            {
              "size": 0,
              "query": {
                "term": {
                  "_slice": "s1"
                }
              }
            }""");
        Response termResponse = getRestClient().performRequest(termBySlice);
        ObjectPath termResponsePath = ObjectPath.createFromResponse(termResponse);
        assertThat(termResponsePath.evaluate("hits.total.value"), equalTo(0));

        Request prefixBySlice = new Request("GET", "/slice-query-disabled-it/_search");
        prefixBySlice.setJsonEntity("""
            {
              "size": 0,
              "query": {
                "prefix": {
                  "_slice": "s1"
                }
              }
            }""");
        Response prefixResponse = getRestClient().performRequest(prefixBySlice);
        ObjectPath prefixResponsePath = ObjectPath.createFromResponse(prefixResponse);
        assertThat(prefixResponsePath.evaluate("hits.total.value"), equalTo(0));
    }

    public void testSliceQueryThrowsWhenUnmappedFieldsDisallowed() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-query-disabled-strict-unmapped-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false,
                "index.query.parse.allow_unmapped_fields": false
              }
            }""");
        getRestClient().performRequest(create);

        Request index = new Request("POST", "/slice-query-disabled-strict-unmapped-it/_doc/1");
        index.addParameter("routing", "s1");
        index.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(index);
        getRestClient().performRequest(new Request("POST", "/slice-query-disabled-strict-unmapped-it/_refresh"));

        Request termBySlice = new Request("GET", "/slice-query-disabled-strict-unmapped-it/_search");
        termBySlice.setJsonEntity("""
            {
              "size": 0,
              "query": {
                "term": {
                  "_slice": "s1"
                }
              }
            }""");
        ResponseException termException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(termBySlice));
        String termResponse = Streams.copyToString(new InputStreamReader(termException.getResponse().getEntity().getContent(), UTF_8));
        assertThat(termResponse, containsString("No field mapping can be found for the field with name [_slice]"));
    }

    public void testSearchUrlSliceRequiredWhenSliceEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-search-url-required-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexDoc = new Request("POST", "/slice-search-url-required-it/_doc/1");
        indexDoc.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(indexDoc);
        getRestClient().performRequest(new Request("POST", "/slice-search-url-required-it/_refresh"));

        Request searchMissingSlice = new Request("GET", "/slice-search-url-required-it/_search");
        searchMissingSlice.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(searchMissingSlice));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is required when [index.slice.enabled] is true"));
    }

    public void testSearchUrlRoutingRejectedWhenSliceEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-search-url-routing-rejected-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexDoc = new Request("POST", "/slice-search-url-routing-rejected-it/_doc/1");
        indexDoc.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(indexDoc);
        getRestClient().performRequest(new Request("POST", "/slice-search-url-routing-rejected-it/_refresh"));

        Request searchWithRouting = new Request("GET", "/slice-search-url-routing-rejected-it/_search");
        searchWithRouting.addParameter("routing", "r1");
        searchWithRouting.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(searchWithRouting));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[routing] is not allowed when [index.slice.enabled] is true"));
        assertThat(response, containsString("use [_slice] instead"));
    }

    public void testCountUrlSliceRequiredWhenSliceEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-count-url-required-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexDoc = new Request("POST", "/slice-count-url-required-it/_doc/1");
        indexDoc.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(indexDoc);
        getRestClient().performRequest(new Request("POST", "/slice-count-url-required-it/_refresh"));

        Request countMissingSlice = new Request("GET", "/slice-count-url-required-it/_count");
        countMissingSlice.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(countMissingSlice));
        String countError = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(countError, containsString("[_slice] is required when [index.slice.enabled] is true"));
    }

    public void testCountUrlRoutingRejectedWhenSliceEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-count-url-routing-rejected-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexDoc = new Request("POST", "/slice-count-url-routing-rejected-it/_doc/1");
        indexDoc.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(indexDoc);
        getRestClient().performRequest(new Request("POST", "/slice-count-url-routing-rejected-it/_refresh"));

        Request countWithRouting = new Request("GET", "/slice-count-url-routing-rejected-it/_count");
        countWithRouting.addParameter("routing", "r1");
        countWithRouting.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(countWithRouting));
        String countError = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(countError, containsString("[routing] is not allowed when [index.slice.enabled] is true"));
        assertThat(countError, containsString("use [_slice] instead"));
    }

    public void testCountUrlSliceFilterIsAdditiveToQueryFilter() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-count-url-additive-filter-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexS1MatchingQuery = new Request("POST", "/slice-count-url-additive-filter-it/_doc/1");
        indexS1MatchingQuery.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexS1MatchingQuery.setJsonEntity("""
            {
              "category": 1
            }""");
        getRestClient().performRequest(indexS1MatchingQuery);

        Request indexS1NonMatchingQuery = new Request("POST", "/slice-count-url-additive-filter-it/_doc/2");
        indexS1NonMatchingQuery.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexS1NonMatchingQuery.setJsonEntity("""
            {
              "category": 2
            }""");
        getRestClient().performRequest(indexS1NonMatchingQuery);

        Request indexS2MatchingQuery = new Request("POST", "/slice-count-url-additive-filter-it/_doc/3");
        indexS2MatchingQuery.addParameter(SliceIndexing.PARAM_NAME, "s2");
        indexS2MatchingQuery.setJsonEntity("""
            {
              "category": 1
            }""");
        getRestClient().performRequest(indexS2MatchingQuery);
        getRestClient().performRequest(new Request("POST", "/slice-count-url-additive-filter-it/_refresh"));

        Request countWithSliceAndQuery = new Request("GET", "/slice-count-url-additive-filter-it/_count");
        countWithSliceAndQuery.addParameter(SliceIndexing.PARAM_NAME, "s1");
        countWithSliceAndQuery.setJsonEntity("""
            {
              "query": {
                "term": {
                  "category": {
                    "value": 1
                  }
                }
              }
            }""");
        Response countResponse = getRestClient().performRequest(countWithSliceAndQuery);
        ObjectPath objectPath = ObjectPath.createFromResponse(countResponse);
        assertThat(objectPath.evaluate("count"), equalTo(1));
    }

    public void testSearchPitRejectedWhenSliceEnabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-search-pit-rejected-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexDoc = new Request("POST", "/slice-search-pit-rejected-it/_doc/1");
        indexDoc.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(indexDoc);
        getRestClient().performRequest(new Request("POST", "/slice-search-pit-rejected-it/_refresh"));

        Request openPit = new Request("POST", "/slice-search-pit-rejected-it/_pit");
        openPit.addParameter("keep_alive", "1m");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(openPit));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is required when [index.slice.enabled] is true"));
    }

    public void testSearchUrlSliceRejectedWhenNoSliceEnabledIndices() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-search-url-disabled-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexDoc = new Request("POST", "/slice-search-url-disabled-it/_doc/1");
        indexDoc.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(indexDoc);
        getRestClient().performRequest(new Request("POST", "/slice-search-url-disabled-it/_refresh"));

        Request searchWithSlice = new Request("GET", "/slice-search-url-disabled-it/_search");
        searchWithSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
        searchWithSlice.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(searchWithSlice));
        String response = Streams.copyToString(new InputStreamReader(exception.getResponse().getEntity().getContent(), UTF_8));
        assertThat(response, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }

    public void testSearchUrlSliceAcceptedForMixedIndicesAndAppliesGlobalFilter() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request createEnabled = new Request("PUT", "/slice-search-url-mixed-enabled-it");
        createEnabled.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(createEnabled);
        Request createDisabled = new Request("PUT", "/slice-search-url-mixed-disabled-it");
        createDisabled.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(createDisabled);

        Request indexS1 = new Request("POST", "/slice-search-url-mixed-enabled-it/_doc/1");
        indexS1.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexS1.setJsonEntity("""
            {
              "field": "a"
            }""");
        getRestClient().performRequest(indexS1);
        Request indexS2 = new Request("POST", "/slice-search-url-mixed-enabled-it/_doc/2");
        indexS2.addParameter(SliceIndexing.PARAM_NAME, "s2");
        indexS2.setJsonEntity("""
            {
              "field": "b"
            }""");
        getRestClient().performRequest(indexS2);

        Request indexDisabled = new Request("POST", "/slice-search-url-mixed-disabled-it/_doc/3");
        indexDisabled.setJsonEntity("""
            {
              "field": "c"
            }""");
        getRestClient().performRequest(indexDisabled);
        getRestClient().performRequest(
            new Request("POST", "/slice-search-url-mixed-enabled-it,slice-search-url-mixed-disabled-it/_refresh")
        );

        Request searchWithSlice = new Request("GET", "/slice-search-url-mixed-enabled-it,slice-search-url-mixed-disabled-it/_search");
        searchWithSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
        searchWithSlice.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              }
            }""");
        Response response = getRestClient().performRequest(searchWithSlice);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("hits.total.value"), equalTo(1));
        assertThat(objectPath.evaluate("hits.hits.0._routing"), equalTo("s1"));
    }

    public void testSearchUrlSliceFilterIsAdditiveToQueryFilter() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-search-url-additive-filter-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexS1MatchingQuery = new Request("POST", "/slice-search-url-additive-filter-it/_doc/1");
        indexS1MatchingQuery.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexS1MatchingQuery.setJsonEntity("""
            {
              "category": 1
            }""");
        getRestClient().performRequest(indexS1MatchingQuery);

        Request indexS1NonMatchingQuery = new Request("POST", "/slice-search-url-additive-filter-it/_doc/2");
        indexS1NonMatchingQuery.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexS1NonMatchingQuery.setJsonEntity("""
            {
              "category": 2
            }""");
        getRestClient().performRequest(indexS1NonMatchingQuery);

        Request indexS2MatchingQuery = new Request("POST", "/slice-search-url-additive-filter-it/_doc/3");
        indexS2MatchingQuery.addParameter(SliceIndexing.PARAM_NAME, "s2");
        indexS2MatchingQuery.setJsonEntity("""
            {
              "category": 1
            }""");
        getRestClient().performRequest(indexS2MatchingQuery);
        getRestClient().performRequest(new Request("POST", "/slice-search-url-additive-filter-it/_refresh"));

        Request searchWithSliceAndQuery = new Request("GET", "/slice-search-url-additive-filter-it/_search");
        searchWithSliceAndQuery.addParameter(SliceIndexing.PARAM_NAME, "s1");
        searchWithSliceAndQuery.setJsonEntity("""
            {
              "query": {
                "term": {
                  "category": 1
                }
              }
            }""");
        Response response = getRestClient().performRequest(searchWithSliceAndQuery);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("hits.total.value"), equalTo(1));
        assertThat(objectPath.evaluate("hits.hits.0._id"), equalTo("1"));
    }

    public void testSearchUrlSliceSupportsMultipleValues() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-search-url-multiple-values-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexS1 = new Request("POST", "/slice-search-url-multiple-values-it/_doc/1");
        indexS1.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexS1.setJsonEntity("""
            {
              "field": "a",
              "seq": 1
            }""");
        getRestClient().performRequest(indexS1);

        Request indexS2 = new Request("POST", "/slice-search-url-multiple-values-it/_doc/2");
        indexS2.addParameter(SliceIndexing.PARAM_NAME, "s2");
        indexS2.setJsonEntity("""
            {
              "field": "b",
              "seq": 2
            }""");
        getRestClient().performRequest(indexS2);

        Request indexS3 = new Request("POST", "/slice-search-url-multiple-values-it/_doc/3");
        indexS3.addParameter(SliceIndexing.PARAM_NAME, "s3");
        indexS3.setJsonEntity("""
            {
              "field": "c",
              "seq": 3
            }""");
        getRestClient().performRequest(indexS3);
        getRestClient().performRequest(new Request("POST", "/slice-search-url-multiple-values-it/_refresh"));

        Request searchWithMultipleSlices = new Request("GET", "/slice-search-url-multiple-values-it/_search");
        searchWithMultipleSlices.addParameter(SliceIndexing.PARAM_NAME, "s1,s2");
        searchWithMultipleSlices.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              },
              "sort": [
                {
                  "seq": {
                    "order": "asc"
                  }
                }
              ]
            }""");
        Response response = getRestClient().performRequest(searchWithMultipleSlices);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("hits.total.value"), equalTo(2));
        assertThat(objectPath.evaluate("hits.hits.0._id"), equalTo("1"));
        assertThat(objectPath.evaluate("hits.hits.1._id"), equalTo("2"));
    }

    public void testSearchUrlSliceAllReturnsAllDocumentsAcrossSlices() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-search-url-all-slices-it");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true,
                "number_of_shards": 1
              }
            }""");
        getRestClient().performRequest(create);

        Request indexS1 = new Request("POST", "/slice-search-url-all-slices-it/_doc/1");
        indexS1.addParameter(SliceIndexing.PARAM_NAME, "s1");
        indexS1.setJsonEntity("""
            {
              "field": "a",
              "seq": 1
            }""");
        getRestClient().performRequest(indexS1);

        Request indexS2 = new Request("POST", "/slice-search-url-all-slices-it/_doc/2");
        indexS2.addParameter(SliceIndexing.PARAM_NAME, "s2");
        indexS2.setJsonEntity("""
            {
              "field": "b",
              "seq": 2
            }""");
        getRestClient().performRequest(indexS2);

        Request indexS3 = new Request("POST", "/slice-search-url-all-slices-it/_doc/3");
        indexS3.addParameter(SliceIndexing.PARAM_NAME, "s3");
        indexS3.setJsonEntity("""
            {
              "field": "c",
              "seq": 3
            }""");
        getRestClient().performRequest(indexS3);
        getRestClient().performRequest(new Request("POST", "/slice-search-url-all-slices-it/_refresh"));

        Request searchAllSlices = new Request("GET", "/slice-search-url-all-slices-it/_search");
        searchAllSlices.addParameter(SliceIndexing.PARAM_NAME, SliceIndexing.SLICE_ALL);
        searchAllSlices.setJsonEntity("""
            {
              "query": {
                "match_all": {}
              },
              "sort": [
                {
                  "seq": {
                    "order": "asc"
                  }
                }
              ]
            }""");
        Response response = getRestClient().performRequest(searchAllSlices);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("hits.total.value"), equalTo(3));
        assertThat(objectPath.evaluate("hits.hits.0._id"), equalTo("1"));
        assertThat(objectPath.evaluate("hits.hits.1._id"), equalTo("2"));
        assertThat(objectPath.evaluate("hits.hits.2._id"), equalTo("3"));
    }

    public void testSliceRequirementAndValidationForGetAndDelete() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-get-delete-enabled");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": true
              }
            }""");
        getRestClient().performRequest(create);

        Request index = new Request("POST", "/slice-get-delete-enabled/_doc/1");
        index.addParameter(SliceIndexing.PARAM_NAME, "s1");
        index.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(index);

        Request getMissingSlice = new Request("GET", "/slice-get-delete-enabled/_doc/1");
        ResponseException missingGetSliceException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(getMissingSlice)
        );
        String missingGetSliceResponse = Streams.copyToString(
            new InputStreamReader(missingGetSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(missingGetSliceResponse, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request getWithSlice = new Request("GET", "/slice-get-delete-enabled/_doc/1");
        getWithSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
        Response getResponse = getRestClient().performRequest(getWithSlice);
        ObjectPath getResponsePath = ObjectPath.createFromResponse(getResponse);
        assertThat(getResponsePath.evaluate("found"), equalTo(true));

        Request deleteMissingSlice = new Request("DELETE", "/slice-get-delete-enabled/_doc/1");
        ResponseException missingDeleteSliceException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(deleteMissingSlice)
        );
        String missingDeleteSliceResponse = Streams.copyToString(
            new InputStreamReader(missingDeleteSliceException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(missingDeleteSliceResponse, containsString("[_slice] is required when [index.slice.enabled] is true"));

        Request deleteWithSlice = new Request("DELETE", "/slice-get-delete-enabled/_doc/1");
        deleteWithSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
        Response deleteResponse = getRestClient().performRequest(deleteWithSlice);
        ObjectPath deleteResponsePath = ObjectPath.createFromResponse(deleteResponse);
        assertThat(deleteResponsePath.evaluate("result"), equalTo("deleted"));
    }

    public void testSliceRejectedForGetAndDeleteWhenSettingDisabled() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        Request create = new Request("PUT", "/slice-get-delete-disabled");
        create.setJsonEntity("""
            {
              "settings": {
                "index.slice.enabled": false
              }
            }""");
        getRestClient().performRequest(create);

        Request index = new Request("POST", "/slice-get-delete-disabled/_doc/1");
        index.setJsonEntity("""
            {
              "field": "value"
            }""");
        getRestClient().performRequest(index);

        Request getWithSlice = new Request("GET", "/slice-get-delete-disabled/_doc/1");
        getWithSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
        ResponseException getException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(getWithSlice));
        String getResponse = Streams.copyToString(new InputStreamReader(getException.getResponse().getEntity().getContent(), UTF_8));
        assertThat(getResponse, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));

        Request deleteWithSlice = new Request("DELETE", "/slice-get-delete-disabled/_doc/1");
        deleteWithSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
        ResponseException deleteException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(deleteWithSlice));
        String deleteResponse = Streams.copyToString(new InputStreamReader(deleteException.getResponse().getEntity().getContent(), UTF_8));
        assertThat(deleteResponse, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
    }

    public void testSliceBehaviorRespectsIndexTemplateSetting() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());

        final String disabledTemplateName = "slice-template-disabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String disabledPattern = "slice-template-disabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT) + "-*";
        final String disabledIndex = disabledPattern.replace("*", "000001");
        Request putDisabledTemplate = new Request("PUT", "/_index_template/" + disabledTemplateName);
        putDisabledTemplate.setJsonEntity(String.format(Locale.ROOT, """
            {
              "index_patterns": ["%s"],
              "template": {
                "settings": {
                  "index.slice.enabled": false
                }
              }
            }""", disabledPattern));
        getRestClient().performRequest(putDisabledTemplate);

        Request disabledSliceWrite = new Request("POST", "/" + disabledIndex + "/_doc/1");
        disabledSliceWrite.addParameter(SliceIndexing.PARAM_NAME, "s1");
        disabledSliceWrite.setJsonEntity("""
            {
              "field": "value"
            }""");
        ResponseException disabledException = expectThrows(
            ResponseException.class,
            () -> getRestClient().performRequest(disabledSliceWrite)
        );
        String disabledResponse = Streams.copyToString(
            new InputStreamReader(disabledException.getResponse().getEntity().getContent(), UTF_8)
        );
        assertThat(disabledResponse, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));

        final String enabledTemplateName = "slice-template-enabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        final String enabledPattern = "slice-template-enabled-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT) + "-*";
        final String enabledIndex = enabledPattern.replace("*", "000001");
        Request putEnabledTemplate = new Request("PUT", "/_index_template/" + enabledTemplateName);
        putEnabledTemplate.setJsonEntity(String.format(Locale.ROOT, """
            {
              "index_patterns": ["%s"],
              "template": {
                "settings": {
                  "index.slice.enabled": true
                }
              }
            }""", enabledPattern));
        getRestClient().performRequest(putEnabledTemplate);

        Request enabledSliceWrite = new Request("POST", "/" + enabledIndex + "/_doc/1");
        enabledSliceWrite.addParameter(SliceIndexing.PARAM_NAME, "s1");
        enabledSliceWrite.setJsonEntity("""
            {
              "field": "value"
            }""");
        Response enabledResponse = getRestClient().performRequest(enabledSliceWrite);
        ObjectPath enabledPath = ObjectPath.createFromResponse(enabledResponse);
        assertThat(enabledPath.evaluate("result"), equalTo("created"));
    }

    public void testSliceProvenanceValidationViaCoordinatingOnlyNode() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        final RestClient restClient = getRestClient();
        final List<Node> originalNodes = restClient.getNodes();
        try {
            restClient.setNodes(
                Collections.singletonList(
                    new Node(
                        HttpHost.create(
                            internalCluster().getInstance(HttpServerTransport.class, coordinatorNode)
                                .boundAddress()
                                .publishAddress()
                                .toString()
                        )
                    )
                )
            );

            Request createEnabled = new Request("PUT", "/slice-provenance-enabled");
            createEnabled.setJsonEntity("""
                {
                  "settings": {
                    "index.slice.enabled": true
                  }
                }""");
            restClient.performRequest(createEnabled);

            Request missingSlice = new Request("POST", "/slice-provenance-enabled/_doc/1");
            missingSlice.setJsonEntity("""
                {
                  "field": "value"
                }""");
            ResponseException missingSliceException = expectThrows(ResponseException.class, () -> restClient.performRequest(missingSlice));
            String missingSliceBody = Streams.copyToString(
                new InputStreamReader(missingSliceException.getResponse().getEntity().getContent(), UTF_8)
            );
            assertThat(missingSliceBody, containsString("[_slice] is required when [index.slice.enabled] is true"));

            Request validSlice = new Request("POST", "/slice-provenance-enabled/_doc/2");
            validSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
            validSlice.setJsonEntity("""
                {
                  "field": "value"
                }""");
            Response createdResponse = restClient.performRequest(validSlice);
            ObjectPath createdPath = ObjectPath.createFromResponse(createdResponse);
            assertThat(createdPath.evaluate("result"), equalTo("created"));

            Request createDisabled = new Request("PUT", "/slice-provenance-disabled");
            createDisabled.setJsonEntity("""
                {
                  "settings": {
                    "index.slice.enabled": false
                  }
                }""");
            restClient.performRequest(createDisabled);

            Request disabledSlice = new Request("POST", "/slice-provenance-disabled/_doc/1");
            disabledSlice.addParameter(SliceIndexing.PARAM_NAME, "s1");
            disabledSlice.setJsonEntity("""
                {
                  "field": "value"
                }""");
            ResponseException disabledSliceException = expectThrows(
                ResponseException.class,
                () -> restClient.performRequest(disabledSlice)
            );
            String disabledSliceBody = Streams.copyToString(
                new InputStreamReader(disabledSliceException.getResponse().getEntity().getContent(), UTF_8)
            );
            assertThat(disabledSliceBody, containsString("[_slice] is not allowed when [index.slice.enabled] is false"));
        } finally {
            restClient.setNodes(originalNodes);
        }
    }
}
