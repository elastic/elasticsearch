/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpFixture;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

import static fixture.aws.AwsCredentialsUtils.ANY_REGION;
import static fixture.aws.AwsCredentialsUtils.mutableAccessKey;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;

public class RepositoryS3RestReloadCredentialsIT extends ESRestTestCase {

    private static final String HASHED_SEED = Integer.toString(Murmur3HashFunction.hash(System.getProperty("tests.seed")));
    private static final String BUCKET = "RepositoryS3RestReloadCredentialsIT-bucket-" + HASHED_SEED;
    private static final String BASE_PATH = "RepositoryS3RestReloadCredentialsIT-base-path-" + HASHED_SEED;

    private static volatile String repositoryAccessKey;

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        BUCKET,
        BASE_PATH,
        mutableAccessKey(() -> repositoryAccessKey, ANY_REGION, "s3")
    );

    private static final MutableSettingsProvider keystoreSettings = new MutableSettingsProvider();

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore(keystoreSettings)
        .setting("s3.client.default.endpoint", s3Fixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testReloadCredentialsFromKeystore() throws IOException {
        // Register repository (?verify=false because we don't have access to the blob store yet)
        final var repositoryName = randomIdentifier();
        registerRepository(
            repositoryName,
            S3Repository.TYPE,
            false,
            Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH).build()
        );
        final var verifyRequest = new Request("POST", "/_snapshot/" + repositoryName + "/_verify");

        // Set up initial credentials
        final var accessKey1 = randomIdentifier();
        repositoryAccessKey = accessKey1;
        keystoreSettings.put("s3.client.default.access_key", accessKey1);
        keystoreSettings.put("s3.client.default.secret_key", randomSecretKey());
        cluster.updateStoredSecureSettings();

        assertOK(client().performRequest(createReloadSecureSettingsRequest()));

        // Check access using initial credentials
        assertOK(client().performRequest(verifyRequest));

        // Rotate credentials in blob store
        final var accessKey2 = randomValueOtherThan(accessKey1, ESTestCase::randomSecretKey);
        repositoryAccessKey = accessKey2;

        // Ensure that initial credentials now invalid
        final var accessDeniedException2 = expectThrows(ResponseException.class, () -> client().performRequest(verifyRequest));
        assertThat(accessDeniedException2.getResponse().getStatusLine().getStatusCode(), equalTo(500));
        assertThat(accessDeniedException2.getMessage(), allOf(containsString("Access denied"), containsString("Status Code: 403")));

        // Set up refreshed credentials
        keystoreSettings.put("s3.client.default.access_key", accessKey2);
        cluster.updateStoredSecureSettings();
        assertOK(client().performRequest(createReloadSecureSettingsRequest()));

        // Check access using refreshed credentials
        assertOK(client().performRequest(verifyRequest));
    }
}
