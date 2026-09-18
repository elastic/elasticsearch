/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.repositories.blobstore.testkit.analyze;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Objects;

import static org.hamcrest.Matchers.hasSize;

public class S3RepositoryAnalysisRestIT extends AbstractS3RepositoryAnalysisRestTestCase {

    public static final S3HttpFixture s3Fixture = new RepositoryAnalysisHttpFixture(S3ConsistencyModel.AWS_DEFAULT);

    public static final ElasticsearchCluster cluster = buildCluster(s3Fixture);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    S3ConsistencyModel consistencyModel() {
        return S3ConsistencyModel.AWS_DEFAULT;
    }

    @Override
    protected boolean checkOverwriteProtection() {
        try {
            // sometimes the repository is registered as not supporting conditional writes, in which case we must suppress overwrite checks
            final var response = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "_snapshot/_all")));
            final var repositories = response.evaluateMapKeys("");
            assertThat(repositories, hasSize(1));
            return Objects.equals(
                "true",
                response.evaluateExact(repositories.iterator().next(), "settings", "unsafely_incompatible_with_s3_conditional_writes")
            ) == false;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
