/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3.qa;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.AbstractSameLengthOverwriteTestCase;
import org.elasticsearch.xpack.esql.datasources.BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3BackendFixture;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.DataSourcesS3HttpFixture;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

/**
 * {@link AbstractSameLengthOverwriteTestCase} on S3. A listing or a probe reports the object's strong ETag, and the first read
 * sends it as {@code If-Match}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class ParquetSameLengthOverwriteIT extends AbstractSameLengthOverwriteTestCase {

    static final DataSourcesS3HttpFixture fixture = new DataSourcesS3HttpFixture();
    private static final S3BackendFixture backend = new S3BackendFixture(fixture);
    private static final ElasticsearchCluster cluster = externalDatasetCluster(SHORT_LISTING_TTL).build();

    @ClassRule
    public static TestRule ruleChain = externalDatasetRules("S3", fixture, cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected BackendFixture backend() {
        return backend;
    }
}
