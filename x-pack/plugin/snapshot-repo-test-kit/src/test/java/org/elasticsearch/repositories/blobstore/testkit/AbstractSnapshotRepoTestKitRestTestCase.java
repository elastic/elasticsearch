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

        final Request request = new Request(HttpPost.METHOD_NAME, "/_snapshot/" + repository + "/_analyze");
        request.addParameter("blob_count", "10");
        request.addParameter("concurrency", "4");
        request.addParameter("max_blob_size", "1mb");
        request.addParameter("timeout", "120s");
        request.addParameter("seed", Long.toString(randomLong()));
        assertOK(client().performRequest(request));
    }

}
