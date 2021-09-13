/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class BWCCodecIT extends ESRestTestCase {
    private static final int DOCS = 5;

    private void oldEsTestCase(String portPropertyName, String requestsPerSecond) throws IOException {
        boolean enabled = Booleans.parseBoolean(System.getProperty("tests.fromOld"));
        assumeTrue("test is disabled, probably because this is windows", enabled);
        String repoLocation = System.getProperty("tests.repoLocation");
        repoLocation = repoLocation + "/" + randomAlphaOfLength(10);

        int oldEsPort = Integer.parseInt(System.getProperty(portPropertyName));
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {
            try {
                Request createIndex = new Request("PUT", "/test");
                createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\": 1}}");
                oldEs.performRequest(createIndex);

                for (int i = 0; i < DOCS; i++) {
                    Request doc = new Request("PUT", "/test/doc/testdoc" + i);
                    doc.addParameter("refresh", "true");
                    doc.setJsonEntity("{\"test\":\"test" + i + "\", \"val\":" + i + "}");
                    oldEs.performRequest(doc);
                }

                // register repo on old ES and take snapshot
                Request createRepoRequest = new Request("PUT", "/_snapshot/testrepo");
                createRepoRequest.setJsonEntity("{\"type\":\"fs\",\"settings\":{\"location\":\"" + repoLocation + "\"}}");
                oldEs.performRequest(createRepoRequest);

                Request createSnapshotRequest = new Request("PUT", "/_snapshot/testrepo/snap1");
                createSnapshotRequest.addParameter("wait_for_completion", "true");
                oldEs.performRequest(createSnapshotRequest);

                // register repo on new ES
                Request createReadRepoRequest = new Request("PUT", "/_snapshot/testrepo");
                createReadRepoRequest.setJsonEntity(
                    "{\"type\":\"lucene5\",\"settings\":{\"delegate_type\":\"fs\",\"location\":\"" + repoLocation +
                        "\",\"readonly\":true}}");
                client().performRequest(createReadRepoRequest);

                // list snapshots on new ES
                Request listSnapshotsRequest = new Request("GET", "/_snapshot/testrepo/snap1");
                listSnapshotsRequest.addParameter("error_trace", "true");
                Response listSnapshotsResponse = client().performRequest(listSnapshotsRequest);
                logger.info(Streams.readFully(listSnapshotsResponse.getEntity().getContent()).utf8ToString());
                assertEquals(200, listSnapshotsResponse.getStatusLine().getStatusCode());

                // list advanced snapshot info on new ES
                Request listSnapshotStatusRequest = new Request("GET", "/_snapshot/testrepo/snap1/_status");
                listSnapshotStatusRequest.addParameter("error_trace", "true");
                Response listSnapshotStatusResponse = client().performRequest(listSnapshotStatusRequest);
                logger.info(Streams.readFully(listSnapshotStatusResponse.getEntity().getContent()).utf8ToString());
                assertEquals(200, listSnapshotStatusResponse.getStatusLine().getStatusCode());

                // restore snapshot on new ES
                Request restoreSnapshotRequest = new Request("POST", "/_snapshot/testrepo/snap1/_restore");
                restoreSnapshotRequest.addParameter("error_trace", "true");
                restoreSnapshotRequest.addParameter("wait_for_completion", "true");
                restoreSnapshotRequest.setJsonEntity("{\"indices\":\"test\"}");
                Response restoreSnapshotResponse = client().performRequest(restoreSnapshotRequest);
                logger.info(Streams.readFully(restoreSnapshotResponse.getEntity().getContent()).utf8ToString());
                assertEquals(200, restoreSnapshotResponse.getStatusLine().getStatusCode());

                Request getMapping = new Request("GET", "/test/_mapping");
                getMapping.addParameter("pretty", "true");
                Response getMappingResponse = client().performRequest(getMapping);
                logger.info(Streams.readFully(getMappingResponse.getEntity().getContent()).utf8ToString());

                // run a search against the restored index
                Request search = new Request("POST", "/test/_search");
                search.addParameter("pretty", "true");
                search.setJsonEntity("{\"stored_fields\": [\"_uid\", \"_source\"]}");
                Response response = client().performRequest(search);
                String result = EntityUtils.toString(response.getEntity());
                logger.info(result);
                for (int i = 0; i < DOCS; i++) {
                    // check that source_ is present
                    assertThat(result, containsString("\"test\" : \"test" + i + "\""));
                    // check that _id is present
                    assertThat(result, containsString("\"_id\" : \"testdoc" + i + "\""));
                }

                // run a search using runtime fields (auto-mapped from old mapping)
                Request runtimeFieldsSearch = new Request("POST", "/test/_search");
                runtimeFieldsSearch.addParameter("pretty", "true");
                runtimeFieldsSearch.setJsonEntity("{\"query\":{\"match\":{\"val\":2}}}");
                Response runtimeFieldsResponse = client().performRequest(runtimeFieldsSearch);
                String runtimeFieldsSearchResult = EntityUtils.toString(runtimeFieldsResponse.getEntity());
                logger.info(runtimeFieldsSearchResult);
                for (int i = 0; i < DOCS; i++) {
                    // check that source_ is present
                    Matcher<String> matcher = containsString("\"test\" : \"test" + i + "\"");
                    if (i != 2) {
                        matcher = not(matcher);
                    }
                    assertThat(runtimeFieldsSearchResult, matcher);
                }

                //assertTrue(false);

            } finally {
                oldEs.performRequest(new Request("DELETE", "/test"));
            }
        }
    }

    public void testEs2() throws IOException {
        oldEsTestCase("es2.port", null);
    }

    public void testEs1() throws IOException {
        oldEsTestCase("es1.port", null);
    }

}
