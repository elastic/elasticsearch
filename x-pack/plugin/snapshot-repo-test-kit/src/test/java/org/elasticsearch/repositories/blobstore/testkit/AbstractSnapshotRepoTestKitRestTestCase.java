/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

public abstract class AbstractSnapshotRepoTestKitRestTestCase extends ESRestTestCase {

    protected abstract String repositoryType();

    protected abstract Settings repositorySettings();

    public void testRepositoryAnalysis() throws Exception {
        final String repositoryType = repositoryType();
        final Settings repositorySettings = repositorySettings();

        final String repository = "repository";
        logger.info("creating repository [{}] of type [{}]", repository, repositoryType);
        registerRepository(repository, repositoryType, true, repositorySettings);

        var request = new Request(HttpPost.METHOD_NAME, "/_snapshot/" + repository + "/_analyze").addParameter("blob_count", "10")
            .addParameter("concurrency", "4")
            .addParameter("max_blob_size", "1mb")
            .addParameter("timeout", "120s")
            .addParameter("seed", Long.toString(randomLong()));
        assertOK(client().performRequest(request));
    }

}
