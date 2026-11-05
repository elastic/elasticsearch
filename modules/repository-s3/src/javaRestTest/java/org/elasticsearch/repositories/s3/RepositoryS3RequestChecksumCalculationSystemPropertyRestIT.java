/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/** The operator asks the SDK to only checksum when required with the {@code aws.requestChecksumCalculation} system property. */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3RequestChecksumCalculationSystemPropertyRestIT extends AbstractRepositoryS3RequestChecksumCalculationRestTestCase {

    private static final S3HttpFixture s3Fixture = newS3Fixture();

    public static ElasticsearchCluster cluster = localClusterWithS3Client(s3Fixture).systemProperty(
        "aws.requestChecksumCalculation",
        "when_required"
    ).build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
