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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;

import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class RepositoryS3WireLoggingRestIT extends AbstractRepositoryS3RestTestCase {

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3WireLoggingRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = PREFIX + "access-key";
    private static final String SECRET_KEY = PREFIX + "secret-key";
    private static final String CLIENT = "wire_logging_client";

    private static final Supplier<String> regionSupplier = new DynamicRegionSupplier();
    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        BUCKET,
        BASE_PATH,
        S3ConsistencyModel::randomConsistencyModel,
        fixedAccessKey(ACCESS_KEY, regionSupplier, "s3")
    );

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .systemProperty("aws.region", regionSupplier)
        .systemProperty("es.insecure_network_trace_enabled", "true")
        .setting("logger.org.apache.http.headers", "DEBUG")
        .setting("logger.org.apache.http.wire", "DEBUG")
        .setting("logger.software.amazon.awssdk.request", "DEBUG")
        .keystore("s3.client." + CLIENT + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT + ".secret_key", SECRET_KEY)
        .setting("s3.client." + CLIENT + ".endpoint", s3Fixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

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

    @Override
    public void testSnapshotAndRestore() throws Exception {
        super.testSnapshotAndRestore();
        try (
            var logReader = new BufferedReader(
                new InputStreamReader(cluster.getNodeLog(0, LogType.SERVER_JSON), StandardCharsets.ISO_8859_1)
            )
        ) {
            final var neededLoggers = new HashSet<>(
                List.of("org.apache.http.wire", "org.apache.http.headers", "software.amazon.awssdk.request")
            );
            String currentLine;

            while ((currentLine = logReader.readLine()) != null && neededLoggers.isEmpty() == false) {
                if (XContentHelper.convertToMap(new BytesArray(currentLine), false, XContentType.JSON)
                    .v2()
                    .get("log.logger") instanceof String loggerName) {
                    neededLoggers.remove(loggerName);
                }
            }
            assertThat(neededLoggers, hasSize(0));
        }
    }
}
