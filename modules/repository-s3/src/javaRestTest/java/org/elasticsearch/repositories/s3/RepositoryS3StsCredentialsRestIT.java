/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.DynamicAwsCredentials;
import fixture.aws.DynamicRegionSupplier;
import fixture.aws.sts.AwsStsHttpFixture;
import fixture.aws.sts.AwsStsHttpHandler;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.function.Supplier;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3StsCredentialsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3StsCredentialsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String CLIENT = "sts_credentials_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final S3HttpFixture s3HttpFixture = new S3HttpFixture(true, BUCKET, BASE_PATH, dynamicCredentials::isAuthorized);

    private static final String WEB_IDENTITY_TOKEN_FILE_CONTENTS = """
        Atza|IQEBLjAsAhRFiXuWpUXuRvQ9PZL3GMFcYevydwIUFAHZwXZXXXXXXXXJnrulxKDHwy87oGKPznh0D6bEQZTSCzyoCtL_8S07pLpr0zMbn6w1lfVZKNTBdDans\
        FBmtGnIsIapjI6xKR02Yc_2bQ8LZbUXSGm6Ry6_BG7PrtLZtj_dfCTj92xNGed-CrKqjG7nPBjNIL016GGvuS5gSvPRUxWES3VYfm1wl7WTI7jn-Pcb6M-buCgHhFO\
        zTQxod27L9CqnOLio7N3gZAGpsp6n1-AJBOCJckcyXe2c6uD0srOJeZlKUm2eTDVMf8IehDVI0r1QOnTV6KzzAI3OY87Vd_cVMQ""";

    private static final AwsStsHttpFixture stsHttpFixture = new AwsStsHttpFixture(
        dynamicCredentials::addValidCredentials,
        WEB_IDENTITY_TOKEN_FILE_CONTENTS
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client." + CLIENT + ".endpoint", s3HttpFixture::getAddress)
        .systemProperty("org.elasticsearch.repositories.s3.stsEndpointOverride", stsHttpFixture::getAddress)
        .configFile(
            S3Service.CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION,
            Resource.fromString(WEB_IDENTITY_TOKEN_FILE_CONTENTS)
        )
        // When running in EKS with container identity the environment variable `AWS_WEB_IDENTITY_TOKEN_FILE` will point to a file which
        // ES cannot access due to its security policy; we override it with `${ES_CONF_PATH}/repository-s3/aws-web-identity-token-file`
        // and require the user to set up a symlink at this location. Thus we can set `AWS_WEB_IDENTITY_TOKEN_FILE` to any old path:
        .environment("AWS_WEB_IDENTITY_TOKEN_FILE", () -> randomIdentifier() + "/" + randomIdentifier())
        // The AWS STS SDK requires the role ARN, it also accepts a session name but will make one up if it's not set.
        // These are checked in AwsStsHttpHandler:
        .environment("AWS_ROLE_ARN", AwsStsHttpHandler.ROLE_ARN)
        .environment("AWS_ROLE_SESSION_NAME", AwsStsHttpHandler.ROLE_NAME)
        // SDKv2 always uses regional endpoints
        .environment("AWS_STS_REGIONAL_ENDPOINTS", () -> randomBoolean() ? "regional" : null)
        .environment("AWS_REGION", regionSupplier)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3HttpFixture).around(stsHttpFixture).around(cluster);

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
