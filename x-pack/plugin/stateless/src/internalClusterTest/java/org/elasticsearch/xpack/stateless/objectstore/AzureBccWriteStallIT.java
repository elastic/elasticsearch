/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.objectstore;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.azure.AzureRepositoryPlugin;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

/**
 * Regression test for incident-3624: when a BCC upload to Azure timed out mid-stream, the SDK used to retry by resetting the upload
 * stream. If the first try was still reading at that point, it would advance the stream past where the retry expected, so the retry
 * uploaded a blob with the wrong content. A shard that recovered from that blob failed to start.
 * <p>
 * The race is forced deterministically: the mock server stalls the first BCC PUT without responding (triggering the SDK timeout and
 * retry), and {@link BlockFirstTryUntilResetHook} pauses the first try until the retry has opened the same compound file, creating
 * the overlap where both tries are active simultaneously.
 */
@SuppressForbidden(reason = "uses HttpServer to emulate Azure storage")
public class AzureBccWriteStallIT extends AbstractStatelessPluginIntegTestCase {

    private static final String ACCOUNT = "account";
    private static final String CONTAINER = "container";

    private static TestObjectStoreServer testServer;
    private static AzureHttpHandler azureHandler;
    private static final StallFirstBccPutHandler stallingHandler = new StallFirstBccPutHandler();

    /** Installed on the VBCCs created while a test runs. Null between tests so uploads are not intercepted. */
    private static final AtomicReference<VirtualBatchedCompoundCommit.UploadStreamHook> uploadStreamHook = new AtomicReference<>();

    @BeforeClass
    public static void startServer() throws IOException {
        testServer = new TestObjectStoreServer();
        azureHandler = new AzureHttpHandler(ACCOUNT, CONTAINER, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE);
        testServer.start();
        testServer.setUp(Map.of("/" + ACCOUNT, stallingHandler));
    }

    @AfterClass
    public static void stopServer() {
        if (testServer != null) {
            testServer.tearDown();
            testServer.stop();
            testServer = null;
        }
    }

    @After
    public void clearUploadStreamHook() {
        uploadStreamHook.set(null);
    }

    @After
    public void releaseStalledBccPut() {
        stallingHandler.release();
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        plugins.add(AzureRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("azure.client.test.account", ACCOUNT);
        // The mock server does not validate HMAC signatures; any base64-encoded value works.
        secureSettings.setString("azure.client.test.key", Base64.getEncoder().encodeToString("test-key".getBytes(StandardCharsets.UTF_8)));

        String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=http://" + testServer.serverUrl() + "/" + ACCOUNT;

        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // upload every commit as its own BCC so the flush in the test triggers the upload right away
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.AZURE)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), CONTAINER)
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .put("azure.client.test.endpoint_suffix", endpoint)
            // per-try timeout: fires when the server stalls without responding and triggers the SDK retry
            .put("azure.client.test.timeout", "2s")
            .put("azure.client.test.max_retries", 2)
            .setSecureSettings(secureSettings);
    }

    /**
     * The server stalls the first BCC PUT until the SDK times out and retries. The hook keeps the first try busy until
     * the retry has opened the same compound file, creating the overlap that used to produce a corrupt blob. The shard is then
     * relocated to a new node to verify it can recover from the stored BCC — a corrupt blob would cause recovery to fail.
     */
    public void testRetryWhileFirstTryStillReadsUploadsCorrectBcc() throws Exception {
        startMasterAndIndexNode();
        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).build());

        // picked up by the VBCC that the flush below creates
        final var hook = new BlockFirstTryUntilResetHook();
        uploadStreamHook.set(hook);

        // concurrent bulks so the commit contains multiple segments
        var indexingThreads = new ArrayList<Thread>();
        for (int i = 0; i < 5; i++) {
            indexingThreads.add(new Thread(() -> {
                var bulk = client().prepareBulk();
                for (int j = 0; j < 10; j++) {
                    bulk.add(prepareIndex(indexName).setSource("data", randomAlphanumericOfLength(64 * 1024)));
                }
                bulk.get();
            }));
        }

        logger.info("--> indexing");
        for (Thread indexingThread : indexingThreads) {
            indexingThread.start();
        }
        for (Thread thread : indexingThreads) {
            thread.join();
        }

        logger.info("indexed");

        // hold the first BCC PUT so the SDK times out and retries
        var stall = stallingHandler.arm();
        flush(indexName);
        assertTrue("mock Azure server did not receive the BCC PUT", stall.received().await(60, TimeUnit.SECONDS));

        // assert the shard moved: before the fix it would fail to recover from the corrupted BCC
        var newNode = startIndexNode();
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", newNode));
        assertBusy(() -> {
            var primary = clusterService().state().routingTable().index(indexName).shard(0).primaryShard();
            assertThat(primary.state(), equalTo(ShardRoutingState.STARTED));
            assertThat(primary.currentNodeId(), equalTo(getNodeId(newNode)));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Forwards all requests to the Azure handler, except the first BCC PUT after {@link #arm()}: that one is held open without
     * reading the body or responding, simulating a slow server, until the client times out or {@link #release()} is called.
     */
    @SuppressForbidden(reason = "uses HttpServer to emulate Azure storage")
    private static class StallFirstBccPutHandler implements HttpHandler {

        private static final Logger logger = LogManager.getLogger(StallFirstBccPutHandler.class);

        private final AtomicReference<BccPutStall> activeStall = new AtomicReference<>();

        BccPutStall arm() {
            var stall = new BccPutStall(new AtomicBoolean(), new CountDownLatch(1), new CountDownLatch(1));
            activeStall.set(stall);
            return stall;
        }

        void release() {
            var stall = activeStall.getAndSet(null);
            if (stall != null) {
                stall.release().countDown();
            }
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            var stall = activeStall.get();
            var path = exchange.getRequestURI().getPath();
            if (stall != null
                && "PUT".equals(exchange.getRequestMethod())
                && path.contains(StatelessCompoundCommit.PREFIX)
                && stall.stalled().compareAndSet(false, true)) {
                logger.info("--> holding BCC PUT [{}] without reading its body", path);
                stall.received().countDown();
                try {
                    stall.release().await(100, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            } else {
                azureHandler.handle(exchange);
            }
        }
    }

    record BccPutStall(AtomicBoolean stalled, CountDownLatch received, CountDownLatch release) {}

    /**
     * Pauses the first upload try just before it opens the compound file, and holds it there until the retry has opened the same file.
     * This creates the interleaving where both tries are active at the same time.
     */
    private static class BlockFirstTryUntilResetHook implements VirtualBatchedCompoundCommit.UploadStreamHook {

        private static final Logger logger = LogManager.getLogger(BlockFirstTryUntilResetHook.class);

        private final AtomicBoolean firstTryBlocked = new AtomicBoolean();
        // counts down when the retry opens the .cfs file, signalling that both tries are active simultaneously
        private final CountDownLatch resetLatch = new CountDownLatch(1);

        @Override
        public InputStream openInternalFile(String filename, CheckedSupplier<InputStream, IOException> opener) throws IOException {
            if (filename.endsWith(".cfs") && firstTryBlocked.compareAndSet(false, true)) {
                // first try: block until the retry has opened the same file, creating the overlap
                logger.info("--> first try blocked while opening [{}] on [{}]", filename, Thread.currentThread().getName());
                var running = true;
                while (running) {
                    try {
                        resetLatch.await(75, TimeUnit.SECONDS);
                        running = false;
                    } catch (InterruptedException e) {
                        // Simulate uninterruptible IO
                    }
                }
                logger.info("--> first try resumes opening [{}] on [{}]", filename, Thread.currentThread().getName());
            } else if (filename.endsWith(".cfs")) {
                // retry's call: unblock the first try and give it time to make its stray read
                logger.info("--> retry opening [{}] on [{}]", filename, Thread.currentThread().getName());
                resetLatch.countDown();
                safeSleep(500);
            }
            return opener.get();
        }

        @Override
        public void onReset() {
            // the concurrent multipart upload path opens a fresh stream per retry rather than resetting; this must never be called
            assert false : "onReset() called unexpectedly — VBCC upload path may have changed to mark/reset";
        }
    }

    /** Installs {@link #uploadStreamHook} on the VBCCs created by the commit service. */
    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new StatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            ) {
                @Override
                protected VirtualBatchedCompoundCommit.UploadStreamHook uploadStreamHook() {
                    var hook = AzureBccWriteStallIT.uploadStreamHook.get();
                    return hook != null ? hook : super.uploadStreamHook();
                }
            };
        }
    }
}
