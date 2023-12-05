/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.s3.advancedstoragetiering;

import fixture.s3.S3HttpFixture;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

public class S3AdvancedStorageTieringRestTestIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(S3AdvancedStorageTieringRestTestIT.class);

    private static final String BUCKET = "test_bucket";
    private static final String BASE_PATH = "test_base_path";
    private static final String CLIENT_NAME = "advanced_storage_tiering";
    private static final String ACCESS_KEY = "test_access_key";

    public static final S3HttpFixture s3Fixture = new S3HttpFixture(true, BUCKET, BASE_PATH, ACCESS_KEY) {
        @Override
        protected void validateStorageClass(String path, String storageClass) {
            assertEquals(
                path,
                storageClass,
                getLeafBlobName(path).startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX) ? "ONEZONE_IA" : "STANDARD_IA"
            );
        }

        private String getLeafBlobName(String path) {
            final var lastSeparator = path.lastIndexOf("/");
            if (lastSeparator < 0) {
                return path;
            } else {
                return path.substring(lastSeparator + 1);
            }
        }
    };

    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .keystore("s3.client." + CLIENT_NAME + ".access_key", ACCESS_KEY)
        .keystore("s3.client." + CLIENT_NAME + ".secret_key", "s3_test_secret_key")
        .setting("s3.client." + CLIENT_NAME + ".protocol", "http")
        .setting("s3.client." + CLIENT_NAME + ".endpoint", s3Fixture::getAddress)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(s3Fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testAdvancedTiering() throws IOException {
        createIndex("testindex");

        final var indexRequest = new Request(HttpPost.METHOD_NAME, "/testindex/_bulk");
        try (var bodyStream = new BytesStreamOutput()) {
            for (int i = 0; i < 10; i++) {
                try (var metaLine = XContentFactory.jsonBuilder(bodyStream)) {
                    metaLine.startObject().startObject("index").endObject().endObject();
                }
                bodyStream.writeByte((byte) 0x0a);
                try (var docLine = XContentFactory.jsonBuilder(bodyStream)) {
                    docLine.startObject().field("foo", "bar-" + i).endObject();
                }
                bodyStream.writeByte((byte) 0x0a);
            }
            indexRequest.setJsonEntity(bodyStream.bytes().utf8ToString());
        }
        client().performRequest(indexRequest);

        final var repoName = randomIdentifier();

        registerRepository(
            repoName,
            "s3",
            true,
            Settings.builder()
                .put("client", CLIENT_NAME)
                .put("bucket", BUCKET)
                .put("base_path", BASE_PATH)
                .put("storage_class", "onezone_ia")
                .put("metadata_storage_class", "standard_ia")
                .build()
        );

        final var request = new Request(HttpPut.METHOD_NAME, "/_snapshot/" + repoName + '/' + randomIdentifier());
        request.addParameter("wait_for_completion", null);

        final var response = ObjectPath.createFromResponse(assertOK(client().performRequest(request)));
        assertEquals("SUCCESS", response.evaluate("snapshot.state"));
    }
}
