/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.gcs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractSameLengthOverwriteTestCase;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.GcsBackendFixture;
import org.elasticsearch.xpack.esql.datasources.GcsFixtureUtils.DataSourcesGcsHttpFixture;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * {@link AbstractSameLengthOverwriteTestCase} on GCS. A listing or a probe reports the object's generation, and the first
 * read sends it as {@code generationMatch}. The fixture bumps the generation on every write.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class GcsSameLengthOverwriteIT extends AbstractSameLengthOverwriteTestCase {

    static final DataSourcesGcsHttpFixture fixture = new DataSourcesGcsHttpFixture();
    // Built in @BeforeClass: the fixture draws its service-account key from the randomized test context.
    private static GcsBackendFixture backend;
    private static final ElasticsearchCluster cluster = externalDatasetCluster(SHORT_LISTING_TTL).build();

    @ClassRule
    public static TestRule ruleChain = externalDatasetRules("GCS", fixture, cluster);

    @BeforeClass
    public static void initBackend() {
        backend = new GcsBackendFixture(fixture);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected BackendFixture backend() {
        return backend;
    }
}
