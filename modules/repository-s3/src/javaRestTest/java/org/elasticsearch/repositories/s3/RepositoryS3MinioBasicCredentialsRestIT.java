/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.minio.MinioTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.function.Supplier;

import static fixture.aws.DynamicIdentifierSupplier.testClassIdentifierSupplier;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3MinioBasicCredentialsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3MinioBasicCredentialsRestIT");
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "minio_client";

    private static final Supplier<String> bucketSupplier = testClassIdentifierSupplier("bucket");
    private static final Supplier<String> basePathSupplier = testClassIdentifierSupplier("base_path");

    private static final LazyMinioFixture minioFixture = new LazyMinioFixture();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
        .setting("s3.client." + CLIENT + ".endpoint", minioFixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(minioFixture).around(cluster);

    private static class LazyMinioFixture extends ExternalResource {
        private MinioTestContainer minioTestContainer;

        @Override
        protected void before() {
            minioTestContainer = new MinioTestContainer(true, ACCESS_KEY, SECRET_KEY, bucketSupplier.get());
            minioTestContainer.start();
        }

        @Override
        protected void after() {
            minioTestContainer.stop();
        }

        String getAddress() {
            return minioTestContainer.getAddress();
        }
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getBucketName() {
        return bucketSupplier.get();
    }

    @Override
    protected String getBasePath() {
        return basePathSupplier.get();
    }

    @Override
    protected String getClientName() {
        return CLIENT;
    }
}
