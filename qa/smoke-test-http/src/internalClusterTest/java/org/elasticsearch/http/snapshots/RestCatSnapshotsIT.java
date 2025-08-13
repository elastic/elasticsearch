/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.http.snapshots;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.hamcrest.Matchers;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.List;

public class RestCatSnapshotsIT extends AbstractSnapshotRestTestCase {

    public void testCatSnapshotsDefaultsToAllRepositories() throws Exception {
        final String repoName1 = "test-repo-1";
        final String repoName2 = "test-repo-2";
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName1, "fs");
        AbstractSnapshotIntegTestCase.createRepository(logger, repoName2, "fs");
        final int snapshotsRepo1 = randomIntBetween(1, 20);
        AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName1, snapshotsRepo1);
        final int snapshotsRepo2 = randomIntBetween(1, 20);
        AbstractSnapshotIntegTestCase.createNSnapshots(logger, repoName2, snapshotsRepo2);
        final Response response = getRestClient().performRequest(new Request(HttpGet.METHOD_NAME, "/_cat/snapshots"));
        assertEquals(HttpURLConnection.HTTP_OK, response.getStatusLine().getStatusCode());
        final List<String> allLines;
        try (InputStream in = response.getEntity().getContent()) {
            allLines = Streams.readAllLines(in);
        }
        assertThat(allLines, Matchers.hasSize(snapshotsRepo1 + snapshotsRepo2));
        assertEquals(allLines.stream().filter(l -> l.contains(repoName1)).count(), snapshotsRepo1);
        assertEquals(allLines.stream().filter(l -> l.contains(repoName2)).count(), snapshotsRepo2);
    }
}
