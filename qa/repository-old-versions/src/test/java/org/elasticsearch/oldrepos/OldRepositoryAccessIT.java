/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldrepos;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class OldRepositoryAccessIT extends ESRestTestCase {
    @Override
    protected Map<String, List<Map<?, ?>>> wipeSnapshots() {
        return Collections.emptyMap();
    }

    public void testOldRepoAccess() throws IOException {
        String repoLocation = System.getProperty("tests.repo.location");
        Version oldVersion = Version.fromString(System.getProperty("tests.es.version"));

        int oldEsPort = Integer.parseInt(System.getProperty("tests.es.port"));
        try (RestClient oldEs = RestClient.builder(new HttpHost("127.0.0.1", oldEsPort)).build()) {
            try {
                Request createIndex = new Request("PUT", "/test");
                createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\": 1}}");
                oldEs.performRequest(createIndex);

                for (int i = 0; i < 5; i++) {
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
                createReadRepoRequest.setJsonEntity("{\"type\":\"fs\",\"settings\":{\"location\":\"" + repoLocation +
                    "\"}}");
                client().performRequest(createReadRepoRequest);

                // list snapshots on new ES
                Request listSnapshotsRequest = new Request("GET", "/_snapshot/testrepo/_all");
                listSnapshotsRequest.addParameter("error_trace", "true");
                Response listSnapshotsResponse = client().performRequest(listSnapshotsRequest);
                logger.info(Streams.readFully(listSnapshotsResponse.getEntity().getContent()).utf8ToString());
                assertEquals(200, listSnapshotsResponse.getStatusLine().getStatusCode());

                // list specific snapshot on new ES
                Request listSpecificSnapshotsRequest = new Request("GET", "/_snapshot/testrepo/snap1");
                listSpecificSnapshotsRequest.addParameter("error_trace", "true");
                Response listSpecificSnapshotsResponse = client().performRequest(listSnapshotsRequest);
                logger.info(Streams.readFully(listSpecificSnapshotsResponse.getEntity().getContent()).utf8ToString());
                assertEquals(200, listSpecificSnapshotsResponse.getStatusLine().getStatusCode());

                // list advanced snapshot info on new ES
                Request listSnapshotStatusRequest = new Request("GET", "/_snapshot/testrepo/snap1/_status");
                listSnapshotStatusRequest.addParameter("error_trace", "true");
                Response listSnapshotStatusResponse = client().performRequest(listSnapshotStatusRequest);
                logger.info(Streams.readFully(listSnapshotStatusResponse.getEntity().getContent()).utf8ToString());
                assertEquals(200, listSnapshotStatusResponse.getStatusLine().getStatusCode());
            } finally {
                oldEs.performRequest(new Request("DELETE", "/test"));
            }
        }
    }

}
