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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3InsecureCredentialsDeprecationsRestIT extends ESRestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3InsecureCredentialsDeprecationsRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";

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
        .module("x-pack-stack")
        .module("transform")
        .systemProperty("aws.region", regionSupplier)
        .systemProperty("es.allow_insecure_settings", "true")
        .setting("s3.client.default.endpoint", s3Fixture::getAddress)
        .setting("xpack.ml.enabled", "false")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testUpgradeAssistantReportsInsecureCredentials() throws IOException {
        final var repoName = randomIdentifier();
        try (var ignored = registerRepository(repoName)) {
            final var responseObjectPath = assertOKAndCreateObjectPath(
                client().performRequest(new Request("GET", "/_migration/deprecations"))
            );

            final List<Map<String, Object>> repositoryIssues = responseObjectPath.evaluate("repositories." + repoName);
            assertThat(repositoryIssues, hasSize(1));
            final var issue = repositoryIssues.get(0);
            assertThat(issue.get("level"), equalTo("critical"));
            assertThat(issue.get("message"), equalTo("S3 repository stores credentials in insecure repository settings"));
            assertThat(issue.get("url"), equalTo(ReferenceDocs.SECURE_SETTINGS.toString()));
            assertThat(issue.get("details"), equalTo(S3Repository.INSECURE_CREDENTIALS_DEPRECATION_WARNING));
            assertThat(issue.get("resolve_during_rolling_upgrade"), equalTo(false));
        }
    }

    private Closeable registerRepository(String repositoryName) throws IOException {
        final var request = newXContentRequest(
            HttpMethod.PUT,
            "/_snapshot/" + repositoryName,
            (b, p) -> b.field("type", S3Repository.TYPE)
                .startObject("settings")
                .value(
                    Settings.builder()
                        .put("bucket", BUCKET)
                        .put("base_path", BASE_PATH)
                        .put("access_key", ACCESS_KEY)
                        .put("secret_key", SECRET_KEY)
                        .put("canned_acl", "private")
                        .put("storage_class", "standard")
                        .build()
                )
                .endObject()
        );
        request.setOptions(
            expectWarnings(
                "[access_key] setting was deprecated in Elasticsearch and will be removed in a future release. "
                    + "See the breaking changes documentation for the next major version.",
                "[secret_key] setting was deprecated in Elasticsearch and will be removed in a future release. "
                    + "See the breaking changes documentation for the next major version.",
                S3Repository.INSECURE_CREDENTIALS_DEPRECATION_WARNING
            )
        );
        assertOK(client().performRequest(request));
        return () -> assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repositoryName)));
    }

    private static String getIdentifierPrefix(String testSuiteName) {
        return testSuiteName + "-" + Integer.toString((testSuiteName + System.getProperty("tests.seed")).hashCode(), 16) + "-";
    }
}
