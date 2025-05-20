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
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.minio.MinioTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Locale;

import static org.elasticsearch.repositories.s3.S3Service.REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING;
import static org.elasticsearch.repositories.s3.S3Service.REPOSITORY_S3_CAS_TTL_SETTING;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3MinioBasicCredentialsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3MinioBasicCredentialsRestIT").toLowerCase(Locale.ROOT);
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base-path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "minio_client";

    private static final MinioTestContainer minioFixture = new MinioTestContainer(true, ACCESS_KEY, SECRET_KEY, BUCKET);

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
        .setting("s3.client." + CLIENT + ".endpoint", minioFixture::getAddress)
        // Skip listing of pre-existing uploads during a CAS because MinIO sometimes leaks them; also reduce the delay before proceeding
        // TODO do not set these if running a MinIO version in which https://github.com/minio/minio/issues/21189 is fixed
        .setting(REPOSITORY_S3_CAS_TTL_SETTING.getKey(), "-1")
        .setting(REPOSITORY_S3_CAS_ANTI_CONTENTION_DELAY_SETTING.getKey(), "100ms")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(minioFixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected String getBucketName() {
        return BUCKET;
    }

    @Override
    protected String getBasePath() {
        return BASE_PATH;
    }

    @Override
    protected String getClientName() {
        return CLIENT;
    }
}
