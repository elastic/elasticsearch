/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.s3;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsRestTestCase;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class S3SearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    @Override
    protected String writeRepositoryType() {
        return "s3";
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String bucket = System.getProperty("test.s3.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.s3.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "searchable_snapshots").put("bucket", bucket).put("base_path", basePath).build();
    }
}
