/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.util.Version;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;

public class FullClusterRestartLuceneIndexCompatibilityIT extends FullClusterRestartIndexCompatibilityTestCase {

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial");
    }

    public FullClusterRestartLuceneIndexCompatibilityIT(Version version) {
        super(version);
    }

    /**
     * Creates an index and a snapshot on N-2, then restores the snapshot on N.
     */
    public void testRestoreIndex() throws Exception {
        final String repository = suffix("repository");
        final String snapshot = suffix("snapshot");
        final String index = suffix("index");
        final int numDocs = 1234;

        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            logger.debug("--> registering repository [{}]", repository);
            registerRepository(client(), repository, FsRepository.TYPE, true, repositorySettings());

            logger.debug("--> creating index [{}]", index);
            createIndex(
                client(),
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .build()
            );

            logger.debug("--> indexing [{}] docs in [{}]", numDocs, index);
            indexDocs(index, numDocs);

            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);
            return;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            ensureGreen(index);

            assertThat(indexVersion(index), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), index, numDocs);

            logger.debug("--> deleting index [{}]", index);
            deleteIndex(index);
            return;
        }

        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var restoredIndex = suffix("index-restored");
            logger.debug("--> restoring index [{}] as [{}]", index, restoredIndex);

            // Restoring the index will fail as Elasticsearch does not support reading N-2 yet
            var request = new Request("POST", "/_snapshot/" + repository + "/" + snapshot + "/_restore");
            request.addParameter("wait_for_completion", "true");
            request.setJsonEntity(Strings.format("""
                {
                  "indices": "%s",
                  "include_global_state": false,
                  "rename_pattern": "(.+)",
                  "rename_replacement": "%s",
                  "include_aliases": false
                }""", index, restoredIndex));

            var responseException = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertEquals(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), responseException.getResponse().getStatusLine().getStatusCode());
            assertThat(
                responseException.getMessage(),
                allOf(
                    containsString("cannot restore index [[" + index),
                    containsString("because it cannot be upgraded"),
                    containsString("has current compatibility version [" + VERSION_MINUS_2 + '-' + VERSION_MINUS_1.getMajor() + ".0.0]"),
                    containsString("but the minimum compatible version is [" + VERSION_MINUS_1.getMajor() + ".0.0]."),
                    containsString("It should be re-indexed in Elasticsearch " + VERSION_MINUS_1.getMajor() + ".x"),
                    containsString("before upgrading to " + VERSION_CURRENT)
                )
            );
        }
    }
}
