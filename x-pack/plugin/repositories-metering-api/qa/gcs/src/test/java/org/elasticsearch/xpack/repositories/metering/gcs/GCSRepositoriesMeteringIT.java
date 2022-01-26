/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.repositories.metering.gcs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.jdk.JavaVersion;
import org.elasticsearch.xpack.repositories.metering.AbstractRepositoriesMeteringAPIRestTestCase;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

public class GCSRepositoriesMeteringIT extends AbstractRepositoriesMeteringAPIRestTestCase {

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
    protected String repositoryType() {
        return "gcs";
    }

    @Override
    protected Map<String, String> repositoryLocation() {
        return org.elasticsearch.core.Map.of("bucket", getProperty("test.gcs.bucket"), "base_path", getProperty("test.gcs.base_path"));
    }

    @Override
    protected Settings repositorySettings() {
        final String bucket = getProperty("test.gcs.bucket");
        final String basePath = getProperty("test.gcs.base_path");

        return Settings.builder().put("client", "repositories_metering").put("bucket", bucket).put("base_path", basePath).build();
    }

    @Override
    protected Settings updatedRepositorySettings() {
        return Settings.builder().put(repositorySettings()).put("gcs.client.repositories_metering.application_name", "updated").build();
    }

    @Override
    protected List<String> readCounterKeys() {
        return org.elasticsearch.core.List.of("GetObject", "ListObjects");
    }

    @Override
    protected List<String> writeCounterKeys() {
        return org.elasticsearch.core.List.of("InsertObject");
    }
}
