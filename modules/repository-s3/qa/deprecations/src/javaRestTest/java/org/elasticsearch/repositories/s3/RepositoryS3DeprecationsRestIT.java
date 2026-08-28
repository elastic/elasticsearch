/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.aws.DynamicRegionSupplier;
import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;
import io.netty.handler.codec.http.HttpMethod;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3DeprecationsRestIT extends ESRestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3DeprecationsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "deprecations_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();

    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        null,
        BUCKET,
        BASE_PATH,
        S3ConsistencyModel::randomConsistencyModel,
        fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .module("constant-keyword")
        .module("x-pack-deprecation")
        .module("x-pack-ilm")
        .module("transform")
        .systemProperty("aws.region", regionSupplier)
        .systemProperty("es.allow_insecure_settings", "true")
        .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
        .setting("s3.client.default.endpoint", s3Fixture::getAddress)
        .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress)
        .setting("xpack.ml.enabled", "false")
        .setting("cluster.deprecation_indexing.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private static String registerRepository(UnaryOperator<Settings.Builder> settingsUnaryOperator, String... expectedWarnings)
        throws IOException {
        final var repoName = randomRepoName();
        final var request = newXContentRequest(
            HttpMethod.PUT,
            "/_snapshot/" + repoName,
            (b, p) -> b.field("type", S3Repository.TYPE)
                .startObject("settings")
                .value(
                    settingsUnaryOperator.apply(Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH).put("client", CLIENT))
                        .build()
                )
                .endObject()
        );
        request.setOptions(expectWarnings(expectedWarnings));
        assertOK(client().performRequest(request));
        return repoName;
    }

    public void testUpgradeAssistantReportsUnsupportedConditionalWrites() throws IOException {
        final var repoName = registerRepository(
            b -> b.put(S3Repository.UNSAFELY_INCOMPATIBLE_WITH_S3_CONDITIONAL_WRITES.getKey(), randomBoolean()),
            """
                [unsafely_incompatible_with_s3_conditional_writes] setting was deprecated in Elasticsearch and will be removed in a \
                future release. See the breaking changes documentation for the next major version."""
        );
        try {
            assertDeprecationIssue(
                repoName,
                "S3 repository explicitly configures a deprecated conditional writes setting",
                ReferenceDocs.S3_COMPATIBLE_REPOSITORIES,
                S3Repository.UNSAFELY_INCOMPATIBLE_WITH_S3_CONDITIONAL_WRITES_DEPRECATION_WARNING
            );
        } finally {
            assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repoName)));
        }
    }

    public void testUpgradeAssistantReportsInsecureCredentials() throws IOException {
        final var repoName = registerRepository(
            b -> b.put(S3Repository.ACCESS_KEY_SETTING.getKey(), ACCESS_KEY).put(S3Repository.SECRET_KEY_SETTING.getKey(), SECRET_KEY),
            """
                [access_key] setting was deprecated in Elasticsearch and will be removed in a future release. \
                See the breaking changes documentation for the next major version.""",
            """
                [secret_key] setting was deprecated in Elasticsearch and will be removed in a future release. \
                See the breaking changes documentation for the next major version.""",
            S3Repository.INSECURE_CREDENTIALS_DEPRECATION_WARNING
        );
        try {
            assertDeprecationIssue(
                repoName,
                "S3 repository stores credentials in insecure repository settings",
                ReferenceDocs.SECURE_SETTINGS,
                S3Repository.INSECURE_CREDENTIALS_DEPRECATION_WARNING
            );
        } finally {
            assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repoName)));
        }
    }

    private static final String PLACEHOLDER_CLIENT = "placeholder";
    private static final Map<String, String> DEPRECATED_CLIENT_SETTING_TEST_VALUES = Map.of(
        "protocol",
        "https",
        "use_throttle_retries",
        "true",
        "signer_override",
        "test_signer"
    );

    public void testUpgradeAssistantReportsDeprecatedClientSettings() throws IOException {
        for (var deprecatedClientSetting : DEPRECATED_CLIENT_SETTING_TEST_VALUES.entrySet()) {
            final var settingKey = deprecatedClientSetting.getKey();
            final var repoName = registerRepository(b -> b.put(settingKey, deprecatedClientSetting.getValue()), Strings.format("""
                [s3.client.%s.%s] setting was deprecated in Elasticsearch and will be removed in a future release. \
                See the breaking changes documentation for the next major version.""", PLACEHOLDER_CLIENT, settingKey));
            try {
                assertDeprecationIssue(
                    repoName,
                    "S3 repository explicitly configures a deprecated client setting",
                    ReferenceDocs.TROUBLESHOOT_REPOSITORY,
                    S3Repository.deprecatedClientSettingDeprecationWarning(settingKey)
                );
            } finally {
                assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repoName)));
            }
        }
    }

    public void testAllDeprecatedClientSettingsAreCoveredByUpgradeAssistantTest() {
        final Set<String> deprecatedClientSettings = new HashSet<>();
        for (final var setting : S3RepositorySettings.DEPRECATED_CLIENT_SETTINGS) {
            deprecatedClientSettings.add(
                setting.getConcreteSettingForNamespace(PLACEHOLDER_CLIENT)
                    .getKey()
                    .substring(S3ClientSettings.REPOSITORY_CLIENT_SETTINGS_PREFIX.length())
            );
        }
        assertThat(DEPRECATED_CLIENT_SETTING_TEST_VALUES.keySet(), equalTo(deprecatedClientSettings));
    }

    private static void assertDeprecationIssue(String repositoryName, String message, ReferenceDocs referenceDocs, String details)
        throws IOException {
        final var deprecations = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_migration/deprecations")));
        final String issuePath = "repositories." + repositoryName;
        assertThat(deprecations.evaluate(issuePath), hasSize(1));
        assertThat(deprecations.evaluate(issuePath + ".0.level"), equalTo("critical"));
        assertThat(deprecations.evaluate(issuePath + ".0.message"), equalTo(message));
        assertThat(deprecations.evaluate(issuePath + ".0.url"), equalTo(referenceDocs.toString()));
        assertThat(deprecations.evaluate(issuePath + ".0.details"), equalTo(details));
        assertThat(deprecations.evaluate(issuePath + ".0.resolve_during_rolling_upgrade"), equalTo(false));
    }

    private static String getIdentifierPrefix(String testSuiteName) {
        return testSuiteName + "-" + Integer.toString((testSuiteName + System.getProperty("tests.seed")).hashCode(), 16) + "-";
    }
}
