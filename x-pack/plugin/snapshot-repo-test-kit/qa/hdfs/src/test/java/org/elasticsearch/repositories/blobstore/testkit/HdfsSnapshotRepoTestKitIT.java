/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit;

import org.elasticsearch.common.settings.Settings;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class HdfsSnapshotRepoTestKitIT extends AbstractSnapshotRepoTestKitRestTestCase {

    @Override
    protected String repositoryType() {
        return "hdfs";
    }

    @Override
    protected Settings repositorySettings() {
        final String uri = System.getProperty("test.hdfs.uri");
        assertThat(uri, not(blankOrNullString()));

        final String path = System.getProperty("test.hdfs.path");
        assertThat(path, not(blankOrNullString()));

        // Optional based on type of test
        final String principal = System.getProperty("test.krb5.principal.es");

        Settings.Builder repositorySettings = Settings.builder().put("client", "repository_test_kit").put("uri", uri).put("path", path);
        if (principal != null) {
            repositorySettings.put("security.principal", principal);
        }
        return repositorySettings.build();
    }
}
