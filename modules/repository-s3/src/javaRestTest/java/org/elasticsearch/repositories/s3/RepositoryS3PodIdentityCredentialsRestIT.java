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
import fixture.aws.imds.Ec2ImdsHttpFixture;
import fixture.aws.imds.Ec2ImdsServiceBuilder;
import fixture.aws.imds.Ec2ImdsVersion;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Exercises the EKS Pod Identity credentials path. Pod Identity, like ECS task roles, resolves credentials through the AWS
 * SDK's {@code ContainerCredentialsProvider} (reached via {@code DefaultCredentialsProvider}) by calling the endpoint named
 * in {@code AWS_CONTAINER_CREDENTIALS_FULL_URI}, sending the token read from {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE}.
 * In EKS that token path sits outside the plugin's entitlement-grantable area, so the operator points
 * {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE} at the entitled symlink
 * ({@code ${ES_PATH_CONF}/repository-s3/eks-pod-identity-token}) and symlinks the real token there, exactly as they already
 * do for the IRSA web-identity token. {@code repository-s3} only grants read access to that location; it does not override
 * the env var or any system property.
 *
 * <p>If the entitlement grant regresses, {@code ContainerCredentialsProvider} fails to read the token file, credential
 * resolution throws, and the repository operations in {@link AbstractRepositoryS3RestTestCase} fail — which is exactly what
 * this test guards against.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3PodIdentityCredentialsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3PodIdentityCredentialsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String CLIENT = "pod_identity_credentials_client";

    private static final String POD_IDENTITY_TOKEN_FILE_CONTENTS = "test-pod-identity-auth-token-" + UUID.randomUUID();

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final Ec2ImdsHttpFixture podIdentityCredentialsFixture = new Ec2ImdsHttpFixture(
        new Ec2ImdsServiceBuilder(Ec2ImdsVersion.V1).newCredentialsConsumer(dynamicCredentials::addValidCredentials)
            .alternativeCredentialsEndpoints(Set.of("/pod_identity_credentials_endpoint"))
    );

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        null,
        BUCKET,
        BASE_PATH,
        S3ConsistencyModel::randomConsistencyModel,
        dynamicCredentials::isAuthorized
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress)
        // The entitled symlink the operator points the SDK at; in production this symlinks the Kubernetes-injected token.
        .configFile(S3Service.POD_IDENTITY_TOKEN_FILE_LOCATION, Resource.fromString(POD_IDENTITY_TOKEN_FILE_CONTENTS))
        // Operators override the EKS-injected AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE to point at the entitled symlink above
        // (which ES is granted read access to). ${ES_PATH_CONF} is expanded by the test-clusters framework to the node's
        // config dir, so the SDK reads the token straight from the entitled location with no override by S3Service.
        .environment("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "${ES_PATH_CONF}/" + S3Service.POD_IDENTITY_TOKEN_FILE_LOCATION)
        .environment(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            () -> podIdentityCredentialsFixture.getAddress() + "/pod_identity_credentials_endpoint"
        )
        .environment("AWS_REGION", regionSupplier) // Region is supplied by environment variable when running in EKS
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(podIdentityCredentialsFixture).around(cluster);

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
