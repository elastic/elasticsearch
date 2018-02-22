/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.repositories.gcs;

import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.hamcrest.Matchers.containsString;

public class GoogleCloudStorageIT extends ESRestTestCase {

    public void testRepository() throws IOException {
        // Create an index with some documents
        createIndex("docs", Settings.EMPTY);

        final int nbDocs = randomIntBetween(1, 100);
        for (int i = 0; i < nbDocs; i++) {
            assertOK(client().performRequest("POST", "/docs/doc", emptyMap(), new StringEntity("{\"snap\":\"one\"}", APPLICATION_JSON)));

            if (rarely()) {
                assertOK(client().performRequest("POST", "/docs/_refresh"));
            }
        }

        // Register the repository
        assertOK(client().performRequest("PUT", "/_snapshot/repository", emptyMap(),
            new StringEntity("{" +
                    "\"type\": \"" + GoogleCloudStorageRepository.TYPE + "\"," +
                    "\"settings\": {" +
                        "\"bucket\": \"bucket_test\"," +
                        "\"client\": \"integration_test\"" +
                    "}" +
                "}",
                APPLICATION_JSON)));

        // Create a snapshot
        assertOK(client().performRequest("POST", "/_snapshot/repository/snapshot-one?wait_for_completion"));

        // Get the snapshot status
        Response getSnapshotOne = client().performRequest("GET", "/_snapshot/repository/snapshot-one");
        assertThat(EntityUtils.toString(getSnapshotOne.getEntity()), containsString("\"state\":\"SUCCESS\""));

        // Add more docs
        final int nbMoreDocs = randomIntBetween(1, 100);
        for (int i = 0; i < nbMoreDocs; i++) {
            assertOK(client().performRequest("POST", "/docs/doc", emptyMap(), new StringEntity("{\"snap\":\"two\"}", APPLICATION_JSON)));
        }

        // Create another snapshot
        assertOK(client().performRequest("POST", "/_snapshot/repository/snapshot-two?wait_for_completion"));

        // Get the snapshot status
        Response getSnapshotTwo = client().performRequest("GET", "/_snapshot/repository/snapshot-two");
        assertThat(EntityUtils.toString(getSnapshotTwo.getEntity()), containsString("\"state\":\"SUCCESS\""));

        // Delete index
        assertOK(client().performRequest("DELETE", "docs"));

        // Restore the last snapshot
        assertOK(client().performRequest("POST", "/_snapshot/repository/snapshot-two/_restore?wait_for_completion"));

        String countResponse =
            EntityUtils.toString(client().performRequest("GET", "/docs/_count?filter_path=_shards.failed,count").getEntity());
        assertThat(countResponse, containsString("\"_shards\":{\"failed\":0}"));
        assertThat(countResponse, containsString("\"count\":" + (nbDocs + nbMoreDocs)));

        // Delete index
        assertOK(client().performRequest("DELETE", "docs"));

        // Restore the first snapshot
        assertOK(client().performRequest("POST", "/_snapshot/repository/snapshot-one/_restore?wait_for_completion"));

        countResponse = EntityUtils.toString(client().performRequest("GET", "/docs/_count?filter_path=_shards.failed,count").getEntity());
        assertThat(countResponse, containsString("\"_shards\":{\"failed\":0}"));
        assertThat(countResponse, containsString("\"count\":" + nbDocs));

        // Delete repository
        assertOK(client().performRequest("DELETE", "/_snapshot/repository"));
    }
}
