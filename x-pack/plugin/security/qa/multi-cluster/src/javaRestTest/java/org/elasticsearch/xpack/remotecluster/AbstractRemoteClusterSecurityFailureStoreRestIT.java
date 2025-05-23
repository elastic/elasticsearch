/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

abstract class AbstractRemoteClusterSecurityFailureStoreRestIT extends AbstractRemoteClusterSecurityTestCase {

    protected void assertSearchResponseContainsIndices(Response response, String... expectedIndices) throws IOException {
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            assertThat(actualIndices, containsInAnyOrder(expectedIndices));
        } finally {
            searchResponse.decRef();
        }
    }

    protected void setupTestDataStreamOnFulfillingCluster() throws IOException {
        // Create data stream and index some documents
        final Request createComponentTemplate = new Request("PUT", "/_component_template/component1");
        createComponentTemplate.setJsonEntity("""
            {
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "age": {
                                "type": "integer"
                            },
                            "email": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text"
                            }
                        }
                    },
                    "data_stream_options": {
                        "failure_store": {
                            "enabled": true
                        }
                    }
                }
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createComponentTemplate));

        final Request createTemplate = new Request("PUT", "/_index_template/template1");
        createTemplate.setJsonEntity("""
            {
                "index_patterns": ["test*"],
                "data_stream": {},
                "priority": 500,
                "composed_of": ["component1"]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createTemplate));

        final Request createDoc1 = new Request("PUT", "/test1/_doc/1?refresh=true&op_type=create");
        createDoc1.setJsonEntity("""
            {
                "@timestamp": 1,
                "age" : 1,
                "name" : "jack",
                "email" : "jack@example.com"
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createDoc1));

        final Request createDoc2 = new Request("PUT", "/test1/_doc/2?refresh=true&op_type=create");
        createDoc2.setJsonEntity("""
            {
                "@timestamp": 2,
                "age" : "this should be an int",
                "name" : "jack",
                "email" : "jack@example.com"
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createDoc2));
        {
            final Request otherTemplate = new Request("PUT", "/_index_template/other_template");
            otherTemplate.setJsonEntity("""
                {
                    "index_patterns": ["other*"],
                    "data_stream": {},
                    "priority": 500,
                    "composed_of": ["component1"]
                }""");
            assertOK(performRequestAgainstFulfillingCluster(otherTemplate));
        }
        {
            final Request createOtherDoc3 = new Request("PUT", "/other1/_doc/3?refresh=true&op_type=create");
            createOtherDoc3.setJsonEntity("""
                {
                    "@timestamp": 3,
                    "age" : 3,
                    "name" : "jane",
                    "email" : "jane@example.com"
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createOtherDoc3));
        }
        {
            final Request createOtherDoc4 = new Request("PUT", "/other1/_doc/4?refresh=true&op_type=create");
            createOtherDoc4.setJsonEntity("""
                {
                    "@timestamp": 4,
                    "age" : "this should be an int",
                    "name" : "jane",
                    "email" : "jane@example.com"
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createOtherDoc4));
        }
    }

    protected static Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

    protected static Response performRequestWithUser(final String user, final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(user, PASS)));
        return client().performRequest(request);
    }

    @SuppressWarnings("unchecked")
    protected Tuple<List<String>, List<String>> getDataAndFailureIndices(String dataStreamName) throws IOException {
        Request dataStream = new Request("GET", "/_data_stream/" + dataStreamName);
        Response response = performRequestAgainstFulfillingCluster(dataStream);
        Map<String, Object> dataStreams = entityAsMap(response);
        List<String> dataIndexNames = (List<String>) XContentMapValues.extractValue("data_streams.indices.index_name", dataStreams);
        List<String> failureIndexNames = (List<String>) XContentMapValues.extractValue(
            "data_streams.failure_store.indices.index_name",
            dataStreams
        );
        return new Tuple<>(dataIndexNames, failureIndexNames);
    }

    protected Tuple<String, String> getSingleDataAndFailureIndices(String dataStreamName) throws IOException {
        Tuple<List<String>, List<String>> indices = getDataAndFailureIndices(dataStreamName);
        assertThat(indices.v1().size(), equalTo(1));
        assertThat(indices.v2().size(), equalTo(1));
        return new Tuple<>(indices.v1().get(0), indices.v2().get(0));
    }

    protected static void assertSelectorsNotSupported(ResponseException exception) {
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(exception.getMessage(), containsString("Selectors are not yet supported on remote cluster patterns"));
    }

}
