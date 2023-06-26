/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.jdk.JavaVersion;
import org.junit.BeforeClass;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.not;

public class GCSSearchableSnapshotsIT extends AbstractSearchableSnapshotsRestTestCase {

    @BeforeClass
    public static void skipJava8() {
        assumeFalse(
            "This test is flaky on jdk8 - we suspect a JDK bug to trigger some assertion in the HttpServer implementation used "
                + "to emulate the server side logic of Google Cloud Storage. See https://bugs.openjdk.java.net/browse/JDK-8180754, "
                + "https://github.com/elastic/elasticsearch/pull/51933 and https://github.com/elastic/elasticsearch/issues/52906 "
                + "for more background on this issue.",
            JavaVersion.current().equals(JavaVersion.parse("8"))
        );
    }

    @Override
    protected String writeRepositoryType() {
        return "gcs";
    }

    @Override
    protected Settings writeRepositorySettings() {
        final String bucket = System.getProperty("test.gcs.bucket");
        assertThat(bucket, not(blankOrNullString()));

        final String basePath = System.getProperty("test.gcs.base_path");
        assertThat(basePath, not(blankOrNullString()));

        return Settings.builder().put("client", "searchable_snapshots").put("bucket", bucket).put("base_path", basePath).build();
    }
}
