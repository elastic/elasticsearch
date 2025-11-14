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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.sun.net.httpserver.HttpHandler;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static fixture.aws.AwsCredentialsUtils.ANY_REGION;
import static fixture.aws.AwsCredentialsUtils.fixedAccessKey;
import static org.elasticsearch.repositories.s3.AbstractRepositoryS3RestTestCase.getIdentifierPrefix;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
@ThreadLeakScope(ThreadLeakScope.Scope.NONE) // https://github.com/elastic/elasticsearch/issues/102482
public class RepositoryS3TimeoutRestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(RepositoryS3TimeoutRestIT.class);

    private static final String PREFIX = getIdentifierPrefix("RepositoryS3TimeoutRestIT");
    private static final String BUCKET = PREFIX + "bucket";
    private static final String BASE_PATH = PREFIX + "base_path";
    private static final String ACCESS_KEY = "access-key";
    private static final String SECRET_KEY = "secret-key";

    private static final long LARGE_OBJECT_SIZE_IN_BYTES = ByteSizeValue.ofMb(10).getBytes();
    private static final S3HttpFixture s3Fixture = new S3HttpFixture(
        true,
        BUCKET,
        BASE_PATH,
        fixedAccessKey(ACCESS_KEY, ANY_REGION, "s3")
    ) {
        @Override
        protected HttpHandler createHandler() {
            final var delegate = super.createHandler();
            return exchange -> {
                if ("PUT".equals(exchange.getRequestMethod())) {
                    final String headerDecodedContentLength = exchange.getRequestHeaders().getFirst("x-amz-decoded-content-length");
                    if (headerDecodedContentLength != null) {
                        if (Long.parseLong(headerDecodedContentLength) > LARGE_OBJECT_SIZE_IN_BYTES) {
                            logger.info(
                                "--> Simulating server unresponsiveness for request [{} {}] with decoded content length [{}]",
                                exchange.getRequestMethod(),
                                exchange.getRequestURI(),
                                headerDecodedContentLength
                            );
                            safeSleep(TimeValue.ONE_MINUTE);
                            logger.info("--> Done simulating server unresponsiveness");
                        }
                    }
                }
                delegate.handle(exchange);
            };
        }
    };

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("repository-s3")
        .keystore("s3.client.default.access_key", ACCESS_KEY)
        .keystore("s3.client.default.secret_key", SECRET_KEY)
        .setting("s3.client.default.endpoint", s3Fixture::getAddress)
        // Short read timeout to demonstrate it does not work for write time unresponsiveness
        .setting("s3.client.default.read_timeout", "1s")
        .setting("s3.client.default.max_retries", "0")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testWriteTimeout() throws IOException {
        // Register repository (?verify=false because we don't have access to the blob store yet)
        final var repositoryName = randomIdentifier();
        registerRepository(
            repositoryName,
            S3Repository.TYPE,
            false,
            Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH).build()
        );

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());

        final int numDocs = 20;
        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"").append(randomAlphanumericOfLength(1024 * 1024)).append("\"}\n");
        }
        final Request bulkRequest = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        bulkRequest.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(bulkRequest));

        // We need one big file to trigger the right stacktrace
        final var forceMergeRequest = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_forcemerge");
        forceMergeRequest.addParameter("max_num_segments", "1");
        assertOK(client().performRequest(forceMergeRequest));

        final var getSegmentsRequest = new Request(HttpGet.METHOD_NAME, "/" + indexName + "/_segments");
        final var segmentsResponse = assertOKAndCreateObjectPath(client().performRequest(getSegmentsRequest));
        final var segments = (Map<String, Object>) segmentsResponse.evaluate("indices." + indexName + ".shards.0.0.segments");
        assertThat(segments, aMapWithSize(1));
        final long segmentSize = (int) ((Map<String, Object>) segments.values().iterator().next()).get("size_in_bytes");
        assertThat(segmentSize, greaterThan(LARGE_OBJECT_SIZE_IN_BYTES));

        final var snapshotName = randomIdentifier();
        final Request request = new Request(HttpPut.METHOD_NAME, "_snapshot/" + repositoryName + '/' + snapshotName);
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\":[\"" + indexName + "\"], \"include_global_state\":false}");
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(
                    // We don't want to rely on the http client's timeout for this test
                    RequestConfig.custom().setSocketTimeout(-1).build()
                )
        );
        // We expect the shard snapshot to timeout before the sleep on storage server side finishes. Therefore, the response
        // should contain such a failure message.
        // If the shard snapshot did not time out as expected, it will wait till the server side sleep finishes and the snapshot
        // succeeds. In that case, the "snapshot.failures" field will be null and the test fails.
        final var snapshotResponse = assertOKAndCreateObjectPath(client().performRequest(request));
        assertNotNull(snapshotResponse.evaluate("snapshot.failures"));
        assertThat((List<Object>) snapshotResponse.evaluate("snapshot.failures"), hasSize(1));
        assertThat(snapshotResponse.evaluate("snapshot.failures.0.reason"), containsString("timeout"));
    }
}
