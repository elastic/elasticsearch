/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.upgrades;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.search.DocValueFormat;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;

/**
 * This is test is meant to verify that when upgrading from 6.x version to 7.7 or newer it is able to parse date fields with joda pattern.
 *
 * The test is indexing documents and searches with use of joda or java pattern.
 * In order to make sure that serialization logic is used a search call is executed 3 times (using all nodes).
 * It cannot be guaranteed that serialization logic will always be used as it might happen that
 * all shards are allocated on the same node and client is connecting to it.
 * Because of this warnings assertions have to be ignored.
 *
 * A special flag used when serializing {@link DocValueFormat.DateTime#writeTo DocValueFormat.DateTime::writeTo}
 * is used to indicate that an index was created in 6.x and has a joda pattern. The same flag is read when
 * {@link DocValueFormat.DateTime#DateTime(StreamInput)} deserializing.
 * When upgrading from 7.0-7.6 to 7.7 there is no way to tell if a pattern was created in 6.x as this flag cannot be added.
 * Hence a skip assume section in init()
 *
 * @see org.elasticsearch.search.DocValueFormat.DateTime
 */
public class JodaCompatibilityIT extends AbstractRollingTestCase {

    @BeforeClass
    public static void init() {
        assumeTrue("upgrading from 7.0-7.6 will fail parsing joda formats", UPGRADE_FROM_VERSION.before(Version.V_7_0_0));
    }

    public void testJodaBackedDocValueAndDateFields() throws Exception {
        switch (CLUSTER_TYPE) {
            case OLD:
                Request createTestIndex = indexWithDateField("joda_time", "YYYY-MM-dd'T'HH:mm:ssZZ");
                createTestIndex.setOptions(ignoreWarnings());

                Response resp = client().performRequest(createTestIndex);
                assertEquals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());

                postNewDoc("joda_time", 1);

                break;
            case MIXED:
                int minute = Booleans.parseBoolean(System.getProperty("tests.first_round")) ? 2 : 3;
                postNewDoc("joda_time", minute);

                Request search = dateRangeSearch("joda_time");
                search.setOptions(ignoreWarnings());

                performOnAllNodes(search, r -> assertEquals(HttpStatus.SC_OK, r.getStatusLine().getStatusCode()));
                break;
            case UPGRADED:
                postNewDoc("joda_time", 4);

                search = searchWithAgg("joda_time");
                search.setOptions(ignoreWarnings());
                // making sure all nodes were used for search
                performOnAllNodes(search, r -> assertResponseHasAllDocuments(r));
                break;
        }
    }

    public void testJavaBackedDocValueAndDateFields() throws Exception {
        switch (CLUSTER_TYPE) {
            case OLD:
                Request createTestIndex = indexWithDateField("java_time", "8yyyy-MM-dd'T'HH:mm:ssXXX");
                Response resp = client().performRequest(createTestIndex);
                assertEquals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());

                postNewDoc("java_time", 1);

                break;
            case MIXED:
                int minute = Booleans.parseBoolean(System.getProperty("tests.first_round")) ? 2 : 3;
                postNewDoc("java_time", minute);

                Request search = dateRangeSearch("java_time");
                Response searchResp = client().performRequest(search);
                assertEquals(HttpStatus.SC_OK, searchResp.getStatusLine().getStatusCode());
                break;
            case UPGRADED:
                postNewDoc("java_time", 4);

                search = searchWithAgg("java_time");
                // making sure all nodes were used for search
                performOnAllNodes(search, r -> assertResponseHasAllDocuments(r));

                break;
        }
    }

    private RequestOptions ignoreWarnings() {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(WarningsHandler.PERMISSIVE);
        return options.build();
    }

    private void performOnAllNodes(Request search, Consumer<Response> consumer) throws IOException {
        List<Node> nodes = client().getNodes();
        for (Node node : nodes) {
            client().setNodes(Collections.singletonList(node));
            Response response = client().performRequest(search);
            consumer.accept(response);
            assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        }
        client().setNodes(nodes);
    }

    private void assertResponseHasAllDocuments(Response searchResp) {
        assertEquals(HttpStatus.SC_OK, searchResp.getStatusLine().getStatusCode());
        try {
            assertEquals(
                removeWhiteSpace(
                    "{"
                        + "  \"_shards\": {"
                        + "    \"total\": 3,"
                        + "    \"successful\": 3"
                        + "  },"
                        + "  \"hits\": {"
                        + "    \"total\": 4,"
                        + "    \"hits\": ["
                        + "      {"
                        + "        \"_source\": {"
                        + "          \"datetime\": \"2020-01-01T00:00:01+01:00\""
                        + "        }"
                        + "      },"
                        + "      {"
                        + "        \"_source\": {"
                        + "          \"datetime\": \"2020-01-01T00:00:02+01:00\""
                        + "        }"
                        + "      },"
                        + "      {"
                        + "        \"_source\": {"
                        + "          \"datetime\": \"2020-01-01T00:00:03+01:00\""
                        + "        }"
                        + "      },"
                        + "      {"
                        + "        \"_source\": {"
                        + "          \"datetime\": \"2020-01-01T00:00:04+01:00\""
                        + "        }"
                        + "      }"
                        + "    ]"
                        + "  }"
                        + "}"
                ),
                EntityUtils.toString(searchResp.getEntity(), StandardCharsets.UTF_8)
            );
        } catch (IOException e) {
            throw new AssertionError("Exception during response parising", e);
        }
    }

    private String removeWhiteSpace(String input) {
        return input.replaceAll("[\\n\\r\\t\\ ]", "");
    }

    private Request dateRangeSearch(String endpoint) {
        Request search = new Request("GET", endpoint + "/_search");
        search.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        search.addParameter("filter_path", "hits.total,hits.hits._source.datetime,_shards.total,_shards.successful");
        search.setJsonEntity(
            ""
                + "{\n"
                + "  \"track_total_hits\": true,\n"
                + "  \"sort\": \"datetime\",\n"
                + "  \"query\": {\n"
                + "    \"range\": {\n"
                + "      \"datetime\": {\n"
                + "        \"gte\": \"2020-01-01T00:00:00+01:00\",\n"
                + "        \"lte\": \"2020-01-02T00:00:00+01:00\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
        );
        return search;
    }

    private Request searchWithAgg(String endpoint) throws IOException {
        Request search = new Request("GET", endpoint + "/_search");
        search.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
        search.addParameter("filter_path", "hits.total,hits.hits._source.datetime,_shards.total,_shards.successful");

        search.setJsonEntity(
            "{\n"
                + "  \"track_total_hits\": true,\n"
                + "  \"sort\": \"datetime\",\n"
                + "  \"query\": {\n"
                + "    \"range\": {\n"
                + "      \"datetime\": {\n"
                + "        \"gte\": \"2020-01-01T00:00:00+01:00\",\n"
                + "        \"lte\": \"2020-01-02T00:00:00+01:00\"\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  \"aggs\" : {\n"
                + "    \"docs_per_year\" : {\n"
                + "      \"date_histogram\" : {\n"
                + "        \"field\" : \"date\",\n"
                + "        \"calendar_interval\" : \"year\"\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}\n"
        );
        return search;
    }

    private Request indexWithDateField(String indexName, String format) {
        Request createTestIndex = new Request("PUT", indexName);
        createTestIndex.addParameter("include_type_name", "false");
        createTestIndex.setJsonEntity(
            "{\n"
                + "  \"settings\": {\n"
                + "    \"index.number_of_shards\": 3\n"
                + "  },\n"
                + "  \"mappings\": {\n"
                + "      \"properties\": {\n"
                + "        \"datetime\": {\n"
                + "          \"type\": \"date\",\n"
                + "          \"format\": \""
                + format
                + "\"\n"
                + "        }\n"
                + "      }\n"
                + "  }\n"
                + "}"
        );
        return createTestIndex;
    }

    private void postNewDoc(String endpoint, int minute) throws IOException {
        Request putDoc = new Request("POST", endpoint + "/_doc");
        putDoc.addParameter("refresh", "true");
        putDoc.addParameter("wait_for_active_shards", "all");
        putDoc.setJsonEntity("{\n" + "  \"datetime\": \"2020-01-01T00:00:0" + minute + "+01:00\"\n" + "}");
        Response resp = client().performRequest(putDoc);
        assertEquals(HttpStatus.SC_CREATED, resp.getStatusLine().getStatusCode());
    }
}
