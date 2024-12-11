/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;

import java.io.IOException;
import java.util.stream.IntStream;

import static org.elasticsearch.test.rest.ObjectPath.createFromResponse;

public class ArchiveIndexCompatibilityIT extends AbstractLuceneIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public ArchiveIndexCompatibilityIT(Version version) {
        super(version);
    }

    public void testRestoreIndex() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        int numDocs = randomIntBetween(1, 10);

        logger.debug("--> registering repository [{}]", repository);
        registerRepository(
            client(),
            repository,
            FsRepository.TYPE,
            true,
            Settings.builder().put("location", REPOSITORY_PATH.getRoot().getPath()).build()
        );

        if (VERSION_MINUS_2.equals(clusterVersion())) {
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .build()
            );
            addDocuments(client(), index, numDocs);
            createSnapshot(client(), repository, snapshot, true);
            deleteIndex(index);
            return;
        }

        if (VERSION_MINUS_1.equals(clusterVersion())) {
            assertTrue(getIndices(client()).isEmpty());
            restoreSnapshot(client(), repository, snapshot, index);
            assertTrue(getIndices(client()).contains(index));
            return;
        }

        if (VERSION_CURRENT.equals(clusterVersion())) {
            ensureGreen(index);
            assertTrue(getIndices(client()).contains(index));
            assertDocCount(client(), index, numDocs);
        }
    }

    private void addDocuments(RestClient client, String index, int numDocs) throws Exception {
        final var bulks = new StringBuilder();
        IntStream.range(0, numDocs).forEach(n -> bulks.append(Strings.format("""
                {"index":{"_id":"%s","_index":"%s"}}
                {"test":"test"}
                """, n, index)));

        var bulkRequest = new Request("POST", "/_bulk");
        bulkRequest.setJsonEntity(bulks.toString());
        var bulkResponse = client.performRequest(bulkRequest);
        assertOK(bulkResponse);
    }

    private void restoreSnapshot(RestClient client, String repository, String snapshot, String index) throws Exception {
        var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_restore");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(Strings.format("""
                {
                  "indices": "%s",
                  "include_global_state": false,
                  "rename_pattern": "(.+)",
                  "include_aliases": false
                }""", index));
        createFromResponse(client.performRequest(request));
    }

    private String getIndices(RestClient client) throws IOException {
        final Request request = new Request("GET", "_cat/indices");
        Response response = client.performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }
}
