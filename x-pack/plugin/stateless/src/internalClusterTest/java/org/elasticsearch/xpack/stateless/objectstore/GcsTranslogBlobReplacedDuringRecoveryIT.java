/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.blobstore.RetryingInputStream;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicatorReader;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

/// This is a **regression** test that asserts the following data-loss scenario on the high level (i.e., when we ack indexed docs, those
/// docs cannot then go missing) cannot occur:
/// First, we index some docs on a node `n`, and then go to upload the translog. During the translog upload, the first attempt of
/// [org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator.UploadTranslogTask] fails due to a timeout (although the request
/// could still be on the wire, about to hit GCS), then the second attempt of `UploadTranslogTask` successfully uploads the translog and we
/// ack back to the client that the docs were indexed. Now, consider, node `n` crashes and a new node tries to recover from the translog.
/// It reads a few bytes of the translog file but then fails and before it resumes the read, the first attempt goes through and overwrites
/// the translog file as a new generation, so the resume of the read gets a 404. In the past, this surfaced as
/// [java.nio.file.NoSuchFileException] which `TranslogReplicatorReader#createPlan` treated as a deleted file and skipped.
/// As a result, the recovered shard would not find out about the indexed docs. (Note that this scenario never occurred in production.)
@SuppressForbidden(reason = "uses HttpServer to emulate GCS")
public class GcsTranslogBlobReplacedDuringRecoveryIT extends AbstractStatelessPluginIntegTestCase {
    private static final String BUCKET = "bucket";
    private static final TestObjectStoreServer testServer = new TestObjectStoreServer();
    private UnresumableTranslogReadHandler handler;

    @BeforeClass
    public static void startServer() throws IOException {
        testServer.start();
    }

    @AfterClass
    public static void stopServer() {
        testServer.stop();
    }

    @Before
    public void setUpHandlers() {
        handler = new UnresumableTranslogReadHandler(new GoogleCloudStorageHttpHandler(BUCKET));
        testServer.setUp(Map.of("/", handler, "/token", new FakeOAuth2HttpHandler()));
    }

    @After
    public void tearDownHandlers() throws Exception {
        try {
            // the cluster needs to be stopped BEFORE the http server is torn down
            cleanUpCluster();
        } finally {
            testServer.tearDown();
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final Settings.Builder settings = super.nodeSettings();
        settings.put("gcs.client.test.endpoint", "http://" + testServer.serverUrl());
        settings.put("gcs.client.test.token_uri", "http://" + testServer.serverUrl() + "/token");
        settings.put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.GCS);
        settings.put(ObjectStoreService.BUCKET_SETTING.getKey(), BUCKET);
        settings.put(ObjectStoreService.CLIENT_SETTING.getKey(), "test");
        settings.put(disableIndexingDiskAndMemoryControllersNodeSettings());

        final byte[] serviceAccount = TestUtils.createServiceAccount(random(), UUID.randomUUID().toString(), "admin@cluster.com");
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile("gcs.client.test.credentials_file", serviceAccount);
        settings.setSecureSettings(secureSettings);
        return settings;
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicatorReader:DEBUG",
        reason = "a skipped translog file is only reported at DEBUG and this test asserts that this does not happen"
    )
    public void testTranslogBlobReplacedDuringRecoveryFromTranslogFailsAndRecoversOnRetry() throws Exception {
        final var masterNode = startMasterAndIndexNode();
        final var indexNode = startIndexNode();
        ensureStableCluster(2);

        final var indexName = randomIndexName();
        createIndex(
            indexName,
            // keep the shard off the master so that it can only be recovered on the index node started below
            indexSettings(1, 0).put("index.routing.allocation.exclude._name", masterNode)
                // no refresh and no automatic flush: the ack-ed documents must only exist in the translog,
                // so that recovery has to replay them from the object store rather
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1gb")
                .build()
        );
        ensureGreen(indexName);

        final var numDocs = between(10, 50);
        final var bulkResponse = indexDocs(indexName, numDocs);
        assertFalse(bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());

        // start a new index node that would be the recovery node
        startIndexNode();
        ensureStableCluster(3);

        try (MockLog mockLog = MockLog.capture(RetryingInputStream.class, Engine.class, TranslogReplicatorReader.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "refused resume",
                    RetryingInputStream.class.getCanonicalName(),
                    Level.WARN,
                    "failed to resume reading [*] at offset [*] with purpose [*]: the blob changed while it was being read"
                )
            );

            // the first recovery attempt fails due to the failed resume (see `UnresumableTranslogReadHandler` below),
            // but the shard is reallocated and the next recovery succeeds
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "first recovery attempt fails",
                    Engine.class.getCanonicalName(),
                    Level.WARN,
                    "failed engine [failed to recover from translog]"
                )
            );

            // if the translog file where to be skipped during recovery we would've seen this log message
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no translog file is skipped",
                    TranslogReplicatorReader.class.getCanonicalName(),
                    Level.DEBUG,
                    "listed translog [*] not found while searching for shard directory, moving to next file"
                )
            );

            // stop the `indexNode` so that we do the recovery through the translog
            internalCluster().stopNode(indexNode);
            ensureGreen(indexName);
            mockLog.assertAllExpectationsMatched();
        }

        // verify we did not lose any documents that we had ack-ed
        assertThat(findIndexShard(indexName).docStats().getCount(), equalTo((long) numDocs));
    }

    // Used to wrap the GCS calls so that the first translog read fails after reading one byte, and when the client attempts
    // to resume the read it gets a `NOT_FOUND` (i.e., 404) as GCS does for a generation that no longer exists.
    @SuppressForbidden(reason = "uses HttpServer to emulate GCS")
    private static class UnresumableTranslogReadHandler implements HttpHandler {
        private final HttpHandler delegate;
        private final AtomicInteger translogReads = new AtomicInteger();

        UnresumableTranslogReadHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String downloadPrefix = "/download/storage/v1/b/" + BUCKET + "/o/";
            final String path = exchange.getRequestURI().getPath();
            if (exchange.getRequestMethod().equals("GET") && path.startsWith(downloadPrefix)) {
                final String blobName = path.substring(downloadPrefix.length());
                if (blobName.contains("/translog/")) {
                    final int translogRead = translogReads.getAndIncrement();
                    if (translogRead == 0) {
                        // The connection "dies" after the first byte, although we state that we return 1024 bytes. To be close to the
                        // test's scenario, we send one byte so that generation 1 is set (i.e., `setGeneration(lastGeneration)`
                        // in `GoogleCloudStorageBlobStoreServices.getInputStream`), and then when we resume the read we fail
                        // (see `else if` below). Note that we do not send all 1024 bytes at once because then the read
                        // would not need to resume.
                        exchange.getResponseHeaders().add("x-goog-generation", "1");
                        exchange.sendResponseHeaders(RestStatus.OK.getStatus(), 1024);
                        exchange.getResponseBody().write(new byte[] { 1 });
                        exchange.getResponseBody().flush();
                        exchange.close();
                        return;
                    } else if (translogRead == 1) {
                        // the client resumes the read from before, asking for generation 1, but we fail that resumed read (emulating
                        // that the generation 1 does not exist anymore because a newer generation was supposedly written)
                        exchange.sendResponseHeaders(RestStatus.NOT_FOUND.getStatus(), -1);
                        exchange.close();
                        return;
                    }
                }
            }
            delegate.handle(exchange);
        }
    }
}
