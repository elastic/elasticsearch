/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit.analyze;

import fixture.s3.S3HttpFixture;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class S3RepositoryAnalysisRestIT extends AbstractS3RepositoryAnalysisRestTestCase {

    public static final S3HttpFixture s3Fixture = new RepositoryAnalysisHttpFixture();

    public static final ElasticsearchCluster cluster = buildCluster(s3Fixture);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
