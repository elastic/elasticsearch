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
 * in {@code AWS_CONTAINER_CREDENTIALS_FULL_URI}. The difference from {@link RepositoryS3EcsCredentialsRestIT} is the auth
 * token: Pod Identity sets {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE} pointing at a Kubernetes-injected token file that
 * sits outside the plugin's entitlement-grantable area. {@code S3Service} therefore redirects the SDK at the entitled
 * symlink ({@code ${ES_PATH_CONF}/repository-s3/eks-pod-identity-token}) via the
 * {@code aws.containerAuthorizationTokenFile} JVM system property, and the user symlinks the real token there exactly as
 * they already do for the IRSA web-identity token.
 *
 * <p>If either the entitlement grant or the sysprop redirect regresses, {@code ContainerCredentialsProvider} fails to read
 * the token file, credential resolution throws, and the repository operations in {@link AbstractRepositoryS3RestTestCase}
 * fail — which is exactly what this test guards against.
 */
@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3PodIdentityCredentialsRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3PodIdentityCredentialsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String CLIENT = "pod_identity_credentials_client";

    // The plugin only checks AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE for presence before redirecting the SDK at the entitled
    // symlink; the SDK then reads the token from the symlink, so this env value just has to be non-empty. We use the path
    // the EKS Pod Identity webhook injects in production for realism.
    private static final String K8S_INJECTED_TOKEN_FILE = "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token";
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
        // Operator-managed symlink that S3Service redirects the AWS SDK at via the aws.containerAuthorizationTokenFile sysprop.
        .configFile(S3Service.POD_IDENTITY_TOKEN_FILE_LOCATION, Resource.fromString(POD_IDENTITY_TOKEN_FILE_CONTENTS))
        // Presence of this env var triggers the sysprop redirect in S3Service; the value is the Kubernetes-injected path,
        // which ES cannot read directly, so the redirect points the SDK at the entitled symlink configured above instead.
        .environment("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", () -> K8S_INJECTED_TOKEN_FILE)
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
