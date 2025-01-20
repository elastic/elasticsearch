/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.analyze;

import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public abstract class AbstractHdfsRepositoryAnalysisRestIT extends AbstractRepositoryAnalysisRestTestCase {

    @Override
    protected String repositoryType() {
        return "hdfs";
    }

    @Override
    protected Settings repositorySettings() {
        final String uri = "hdfs://localhost:" + getHdfsPort();
        // final String uri = System.getProperty("test.hdfs.uri");
        assertThat(uri, not(blankOrNullString()));

        final String path = getRepositoryPath();
        assertThat(path, not(blankOrNullString()));
        Settings.Builder repositorySettings = Settings.builder().put("client", "repository_test_kit").put("uri", uri).put("path", path);
        return repositorySettings.build();
    }

    protected abstract String getRepositoryPath();

    protected abstract int getHdfsPort();

}
