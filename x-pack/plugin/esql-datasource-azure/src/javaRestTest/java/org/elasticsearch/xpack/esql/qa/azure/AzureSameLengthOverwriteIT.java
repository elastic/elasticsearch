/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.azure;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.AzureReactorThreadFilter;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractSameLengthOverwriteTestCase;
import org.elasticsearch.xpack.esql.datasources.AzureBackendFixture;
import org.elasticsearch.xpack.esql.datasources.AzureFixtureUtils.DataSourcesAzureHttpFixture;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * {@link AbstractSameLengthOverwriteTestCase} on Azure. A listing or a probe reports the blob's ETag, and the first read sends
 * it as {@code If-Match}. List Blobs reports the ETag unquoted while downloads quote it, so the glob cases also check that the
 * two forms are compared as the same version.
 */
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class, AzureReactorThreadFilter.class })
public class AzureSameLengthOverwriteIT extends AbstractSameLengthOverwriteTestCase {

    static final DataSourcesAzureHttpFixture fixture = new DataSourcesAzureHttpFixture();
    private static final AzureBackendFixture backend = new AzureBackendFixture(fixture);
    private static final ElasticsearchCluster cluster = externalDatasetCluster(SHORT_LISTING_TTL).build();

    @ClassRule
    public static TestRule ruleChain = externalDatasetRules("Azure", fixture, cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected BackendFixture backend() {
        return backend;
    }
}
