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
import org.elasticsearch.test.fixtures.minio.MinioFixtureTestContainer;
import org.elasticsearch.test.fixtures.s3.S3Fixture;
import org.elasticsearch.test.fixtures.s3.S3FixtureRule;
import org.elasticsearch.test.fixtures.s3.S3HttpFixtureRule;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.fixtures.s3.S3HttpFixtureRule.S3FixtureType.*;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakAction({ ThreadLeakAction.Action.WARN, ThreadLeakAction.Action.INTERRUPT })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class RepositoryS3ClientYamlTestSuiteIT extends ESClientYamlSuiteTestCase {
    private static final boolean USE_FIXTURE = Booleans.parseBoolean(System.getProperty("tests.s3.fixture", "true"));
    private static final List<String> fixturesS3Services = resolveS3ServicesFromSystemProperties();
    private static final List<String> fixturesMinioServices = resolveMinioServicesFromSystemProperties();
    private static final String s3TemporarySessionToken = "session_token";

    private static S3HttpFixtureRule environment = new S3HttpFixtureRule(USE_FIXTURE, resolveS3HttpFixtures());

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
    // public static S3Fixture environment = configureS3Environment();

    private static S3Fixture configureS3Environment() {
        S3Fixture fixture = new S3FixtureRule(USE_FIXTURE);
        // register services exposed by the fixture
        fixturesS3Services.forEach(fixture::withExposedService);
        return fixture;
    }

    public static ElasticsearchCluster cluster = configureCluster();

    private static ElasticsearchCluster configureCluster() {
        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = ElasticsearchCluster.local().module("repository-s3");
        /*

        if (fixturesMinioServices.contains("minio-fixture")) {
            cluster.keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
                .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"));
            if (USE_FIXTURE) {
                cluster.setting(
                    "s3.client.integration_test_permanent.endpoint",
                    () -> minioFixtureTestContainer.getServiceUrl("minio-fixture")
                );
            }
        }
        else if (fixturesS3Services.contains("s3-fixture-with-ecs")) {
            cluster.setting("s3.client.integration_test_ecs.endpoint", () -> environment.getServiceUrl("s3-fixture-with-ecs"));
            cluster.environment(
                "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                () -> (environment.getServiceUrl("s3-fixture-with-ecs") + "/ecs_credentials_endpoint")
            );
        } else if (fixturesS3Services.contains("s3-fixture-with-sts")) {
            cluster.setting("s3.client.integration_test_sts.endpoint", () -> environment.getServiceUrl("s3-fixture-with-sts"));
            cluster.systemProperty(
                "com.amazonaws.sdk.stsMetadataServiceEndpointOverride",
                () -> environment.getServiceUrl("s3-fixture-with-sts") + "/assume-role-with-web-identity"
            );

            cluster.configFile("repository-s3/aws-web-identity-token-file", Resource.fromClasspath("aws-web-identity-token-file"))
                .environment("AWS_WEB_IDENTITY_TOKEN_FILE", System.getProperty("awsWebIdentityTokenExternalLocation"))
                // The AWS STS SDK requires the role and session names to be set. We can verify that they are sent to S3S in the
                // S3HttpFixtureWithSTS fixture
                .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
                .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test");
        } else if (Booleans.parseBoolean(System.getProperty("tests.sts.regional", "false"))) {
            cluster.configFile("repository-s3/aws-web-identity-token-file", Resource.fromClasspath("aws-web-identity-token-file"))
                .environment("AWS_WEB_IDENTITY_TOKEN_FILE", System.getProperty("awsWebIdentityTokenExternalLocation"))
                // The AWS STS SDK requires the role and session names to be set. We can verify that they are sent to S3S in the
                // S3HttpFixtureWithSTS fixture
                .environment("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/FederatedWebIdentityRole")
                .environment("AWS_ROLE_SESSION_NAME", "sts-fixture-test")
                .environment("AWS_STS_REGIONAL_ENDPOINTS", "regional")
                .environment("AWS_REGION", "ap-southeast-2");
        } else {
        */
        cluster.keystore("s3.client.integration_test_permanent.access_key", System.getProperty("s3PermanentAccessKey"))
            .keystore("s3.client.integration_test_permanent.secret_key", System.getProperty("s3PermanentSecretKey"))
            .keystore("s3.client.integration_test_temporary.access_key", System.getProperty("s3TemporaryAccessKey"))
            .keystore("s3.client.integration_test_temporary.secret_key", System.getProperty("s3TemporarySecretKey"))
            .keystore("s3.client.integration_test_temporary.session_token", s3TemporarySessionToken);

        if (USE_FIXTURE) {
            cluster.setting("s3.client.integration_test_permanent.endpoint", () -> environment.getAddress(S3Fixture))
                .setting("s3.client.integration_test_temporary.endpoint", () -> environment.getAddress(S3FixtureWithToken))
                .setting("s3.client.integration_test_ec2.endpoint", () -> environment.getAddress(S3FixtureEc2))
                .systemProperty("com.amazonaws.sdk.ec2MetadataServiceEndpointOverride", () -> environment.getAddress(S3FixtureEc2));
        }
        return cluster.build();
    }

    public static MinioFixtureTestContainer minioFixtureTestContainer = new MinioFixtureTestContainer(
        resolveMinioServicesFromSystemProperties()
    );

    public static TestRule ruleChain = RuleChain.outerRule(environment).around(cluster);

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
