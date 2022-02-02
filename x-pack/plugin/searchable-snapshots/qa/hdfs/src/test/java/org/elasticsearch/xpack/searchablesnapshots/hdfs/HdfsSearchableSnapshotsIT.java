/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.hdfs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsRestTestCase;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class HdfsSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {
    @Override
    protected String writeRepositoryType() {
        return "hdfs";
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String uri = System.getProperty("test.hdfs.uri");
        assertThat(uri, not(blankOrNullString()));

        final String path = System.getProperty("test.hdfs.path");
        assertThat(path, not(blankOrNullString()));

        // Optional based on type of test
        final String principal = System.getProperty("test.krb5.principal.es");

        Settings.Builder repositorySettings = Settings.builder().put("client", "searchable_snapshots").put("uri", uri).put("path", path);
        if (principal != null) {
            repositorySettings.put("security.principal", principal);
        }
        return repositorySettings.build();
    }
}
