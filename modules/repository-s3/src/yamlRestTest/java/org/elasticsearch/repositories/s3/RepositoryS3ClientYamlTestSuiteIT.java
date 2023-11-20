/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.minio.MinioFixtureTestContainer;
import fixture.s3.junit.S3HttpFixtureRule;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;

import static fixture.s3.junit.S3HttpFixtureRule.S3FixtureType.S3Fixture;
import static fixture.s3.junit.S3HttpFixtureRule.S3FixtureType.S3FixtureEc2;
import static fixture.s3.junit.S3HttpFixtureRule.S3FixtureType.S3FixtureEcs;
import static fixture.s3.junit.S3HttpFixtureRule.S3FixtureType.S3FixtureSts;
import static fixture.s3.junit.S3HttpFixtureRule.S3FixtureType.S3FixtureWithToken;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakAction({ ThreadLeakAction.Action.WARN, ThreadLeakAction.Action.INTERRUPT })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RepositoryS3ClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("tests.s3.fixture", "true"));
    private static final String TEST_TASK = System.getProperty("tests.task");
    private static final List<String> fixturesMinioServices = resolveMinioServicesFromSystemProperties();
    private static final S3HttpFixtureRule s3Environment = new S3HttpFixtureRule(USE_FIXTURE, resolveS3HttpFixtures());
    private static final String s3TemporarySessionToken = "session_token";

    @SuppressForbidden(reason = "Forbidden method invocation: java.lang.System#getProperties()")
    private static List<S3HttpFixtureRule.S3FixtureType> resolveS3HttpFixtures() {
        return System.getProperties()
            .entrySet()
            .stream()
            .filter(entry -> ((String) entry.getKey()).startsWith("fixture.service."))
            .map(Map.Entry::getValue)
            .map(Object::toString)
            .map(S3HttpFixtureRule.S3FixtureType::valueOf)
            .toList();
    }

    public static ElasticsearchCluster cluster = configureCluster();

    public static MinioFixtureTestContainer minioFixtureTestContainer = new MinioFixtureTestContainer(fixturesMinioServices);

    private static ElasticsearchCluster configureCluster() {
        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = ElasticsearchCluster.local().module("repository-s3");

        if (TEST_TASK.endsWith("s3ThirdPartyTest")) {
            cluster.keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
                .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"));
            if (USE_FIXTURE) {
                cluster.setting("s3.client.integration_test_permanent.endpoint", () -> minioFixtureTestContainer.getServiceUrl());
            }
        }
        if (TEST_TASK.endsWith("yamlRestTestECS")) {
            cluster.setting("s3.client.integration_test_ecs.endpoint", () -> s3Environment.getAddress(S3FixtureEcs));
            cluster.environment(
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                () -> (s3Environment.getAddress(S3FixtureEcs) + "/ecs_credentials_endpoint")
            );
        }
        if (TEST_TASK.endsWith("yamlRestTestMinio")) {
            cluster.keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
                .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"))
                .setting("s3.client.integration_test_permanent.endpoint", () -> minioFixtureTestContainer.getServiceUrl());
        }
        if (TEST_TASK.endsWith("yamlRestTestSTS")) {
            cluster.setting("s3.client.integration_test_sts.endpoint", () -> s3Environment.getAddress(S3FixtureSts));
            cluster.systemProperty(
                "com.amazonaws.sdk.stsMetadataServiceEndpointOverride",
                () -> s3Environment.getAddress(S3FixtureSts) + "/assume-role-with-web-identity"
            );
            cluster.configFile("repository-s3/aws-web-identity-token-file", Resource.fromClasspath("aws-web-identity-token-file"))
                .environment("AWS_WEB_IDENTITY_TOKEN_FILE", System.getProperty("awsWebIdentityTokenExternalLocation"))
                // The AWS STS SDK requires the role and session names to be set. We can verify that they are sent to S3S in the
                // S3HttpFixtureWithSTS fixture
                .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
                .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test");
        } else if (TEST_TASK.endsWith("yamlRestTestRegionalSTS")) {
            cluster.configFile("repository-s3/aws-web-identity-token-file", Resource.fromClasspath("aws-web-identity-token-file"))
                .environment("AWS_WEB_IDENTITY_TOKEN_FILE", System.getProperty("awsWebIdentityTokenExternalLocation"))
                // The AWS STS SDK requires the role and session names to be set. We can verify that they are sent to S3S in the
                // S3HttpFixtureWithSTS fixture
                .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
                .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test")
                .environment("AWS_STS_REGIONAL_ENDPOINTS", "regional")
                .environment("AWS_REGION", "ap-southeast-2");
        } else if (TEST_TASK.endsWith("yamlRestTest")) {
            cluster.keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
                .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"))
                .keystore("s3.client.integration_test_temporary.access_key", System.getProperty("s3TemporaryAccessKey"))
                .keystore("s3.client.integration_test_temporary.secret_key", System.getProperty("s3TemporarySecretKey"))
                .keystore("s3.client.integration_test_temporary.session_token", s3TemporarySessionToken);
            if (USE_FIXTURE) {
                cluster.setting("s3.client.integration_test_permanent.endpoint", () -> s3Environment.getAddress(S3Fixture))
                    .setting("s3.client.integration_test_temporary.endpoint", () -> s3Environment.getAddress(S3FixtureWithToken))
                    .setting("s3.client.integration_test_ec2.endpoint", () -> s3Environment.getAddress(S3FixtureEc2))
                    .systemProperty("com.amazonaws.sdk.ec2MetadataServiceEndpointOverride", () -> s3Environment.getAddress(S3FixtureEc2));
            }
        }

        return cluster.build();
    }

    public static TestRule ruleChain = RuleChain.outerRule(s3Environment).around(cluster);

    @ClassRule
    public static TestRule ruleChain2 = RuleChain.outerRule(minioFixtureTestContainer).around(ruleChain);

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @SuppressForbidden(reason = "Forbidden method invocation: java.lang.System#getProperties()")
    private static List<String> resolveS3ServicesFromSystemProperties() {
        return System.getProperties()
            .entrySet()
            .stream()
            .filter(entry -> ((String) entry.getKey()).startsWith("fixture.s3-fixture.service"))
            .map(Map.Entry::getValue)
            .map(Object::toString)
            .toList();
    }

    @SuppressForbidden(reason = "Forbidden method invocation: java.lang.System#getProperties()")
    private static List<String> resolveMinioServicesFromSystemProperties() {
        return System.getProperties()
            .entrySet()
            .stream()
            .filter(entry -> ((String) entry.getKey()).startsWith("fixture.minio-fixture.service"))
            .map(Map.Entry::getValue)
            .map(Object::toString)
            .toList();
    }

    public RepositoryS3ClientYamlTestSuiteIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
