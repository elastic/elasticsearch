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
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import io.netty.handler.codec.http.HttpMethod;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Supplier;

import static org.elasticsearch.repositories.s3.AbstractRepositoryS3RestTestCase.getIdentifierPrefix;
import static org.hamcrest.Matchers.equalTo;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3StsCredentialsReloadRestIT extends ESRestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3StsCredentialsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String CLIENT = "sts_credentials_reload_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final DynamicAwsCredentials dynamicCredentials = new DynamicAwsCredentials(regionSupplier, "s3");

    private static final S3HttpFixture s3HttpFixture = new S3HttpFixture(
        true,
        BUCKET,
        BASE_PATH,
        S3ConsistencyModel::randomConsistencyModel,
        dynamicCredentials::isAuthorized
    );

    private static volatile String expectedWebIdentityTokenFileContents = "first token";

    private static final AwsStsHttpFixture stsHttpFixture = new AwsStsHttpFixture(
        dynamicCredentials::addValidCredentials,
        () -> expectedWebIdentityTokenFileContents,
        TimeValue.timeValueSeconds(115) // SDK tries to refresh credentials if they expire in less than 2 minutes == 120s
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .setting("s3.client." + CLIENT + ".endpoint", s3HttpFixture::getAddress)
        .systemProperty("org.elasticsearch.repositories.s3.stsEndpointOverride", stsHttpFixture::getAddress)
        .configFile(
            S3Service.CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION,
            Resource.fromString("not ready yet")
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

    public void testReloadCredentials() throws IOException {
        final var repositoryName = "repo-" + randomIdentifier();

        // Token file starts out invalid, causing the STS fixture to return 403s rather than the credentials the SDK asks for, so we cannot
        // create the repository:
        assertThat(
            expectThrows(ResponseException.class, () -> client().performRequest(getPutRepositoryRequest(repositoryName))).getResponse()
                .getStatusLine()
                .getStatusCode(),
            equalTo(500)
        );

        // However the S3 SDK just keeps on attempting to get credentials, including re-reading the token file, which we can confirm by
        // replacing the file with valid contents so that the STS fixture starts to return credentials:
        final var webIdentityTokenFilePath = cluster.getNodeConfigPath(0)
            .resolve(S3Service.CustomWebIdentityTokenCredentialsProvider.WEB_IDENTITY_TOKEN_FILE_LOCATION);
        Files.writeString(webIdentityTokenFilePath, expectedWebIdentityTokenFileContents);
        client().performRequest(getPutRepositoryRequest(repositoryName));
        assertVerifySuccess(repositoryName);

        // Moreover the STS fixture is configured to return credentials which have almost expired, so the S3 SDK uses these credentials but
        // also attempts to refresh them. We can confirm this by invalidating all the credentials returned so far and observing that these
        // API calls still succeed.
        dynamicCredentials.clearValidCredentials();
        assertVerifySuccess(repositoryName);

        // Also, if we reconfigure the STS fixture to expect a different token in the token file then it'll start returning 403s to these
        // token-refresh requests, and in this case the SDK continues to use the last-known-good credentials.
        expectedWebIdentityTokenFileContents = randomAlphanumericOfLength(100);
        assertVerifySuccess(repositoryName);

        // However if we now invalidate the last-known-good credentials while the STS fixture is returning 403s then API requests must fail.
        dynamicCredentials.clearValidCredentials();
        assertVerifyFailure(repositoryName);

        // But the SDK keeps on trying, which we can confirm by restore the STS fixture to health again and observe that this is enough to
        // allow API requests to succeed.
        Files.writeString(webIdentityTokenFilePath, expectedWebIdentityTokenFileContents);
        assertVerifySuccess(repositoryName);

    }

    private static void assertVerifyFailure(String repositoryName) {
        assertThat(
            expectThrows(
                ResponseException.class,
                () -> client().performRequest(new Request("POST", "/_snapshot/" + repositoryName + "/_verify"))
            ).getResponse().getStatusLine().getStatusCode(),
            equalTo(500)
        );
    }

    private static void assertVerifySuccess(String repositoryName) throws IOException {
        client().performRequest(new Request("POST", "/_snapshot/" + repositoryName + "/_verify"));
    }

    private static @NotNull Request getPutRepositoryRequest(String repositoryName) throws IOException {
        return newXContentRequest(
            HttpMethod.PUT,
            "/_snapshot/" + repositoryName,
            (b, p) -> b.field("type", S3Repository.TYPE)
                .startObject("settings")
                .value(Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH).put("client", CLIENT).build())
                .endObject()
        );
    }
}
