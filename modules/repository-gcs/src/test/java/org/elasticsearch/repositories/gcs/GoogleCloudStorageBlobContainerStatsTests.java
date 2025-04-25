/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobContainerUtils;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROJECT_ID_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.StorageOperation.GET;
import static org.elasticsearch.repositories.gcs.StorageOperation.INSERT;
import static org.elasticsearch.repositories.gcs.StorageOperation.LIST;

@SuppressForbidden(reason = "Uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageBlobContainerStatsTests extends ESTestCase {
    private static final String BUCKET = "bucket";
    private static final ByteSizeValue BUFFER_SIZE = ByteSizeValue.ofKb(128);

    private HttpServer httpServer;
    private ThreadPool threadPool;

    private GoogleCloudStorageService googleCloudStorageService;
    private GoogleCloudStorageHttpHandler googleCloudStorageHttpHandler;
    private ContainerAndBlobStore containerAndStore;

    // A utility method that prints nicer diff messages, rather than dumping entire map, like this:
    // Stats counts do not match:
    // {SNAPSHOT_DATA_RESUMABLE_UPLOAD=Diff[wantOps=1, gotOps=1, diffOps=0, wantReqs=3, gotReqs=2, diffReqs=1]}
    static void assertStatsEquals(Map<String, BlobStoreActionStats> want, Map<String, BlobStoreActionStats> got) {
        assert want.keySet().equals(got.keySet());
        record Diff(long wantOps, long gotOps, long diffOps, long wantReqs, long gotReqs, long diffReqs) {}
        var diff = new HashMap<String, Diff>();
        for (var wkey : want.keySet()) {
            if (got.containsKey(wkey)) {
                var gotStat = got.get(wkey);
                var wantStat = want.get(wkey);
                if (gotStat.equals(wantStat) == false) {
                    diff.put(
                        wkey,
                        new Diff(
                            wantStat.operations(),
                            gotStat.operations(),
                            wantStat.operations() - gotStat.operations(),
                            wantStat.requests(),
                            gotStat.requests(),
                            wantStat.requests() - gotStat.requests()
                        )
                    );
                }
            }
        }
        if (diff.size() > 0) {
            fail("Stats counts do not match:\n" + diff);
        }
    }

    @Before
    public void createStorageService() throws Exception {
        threadPool = new TestThreadPool(getTestClass().getName());
        httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.start();
        googleCloudStorageService = new GoogleCloudStorageService();
        googleCloudStorageHttpHandler = new GoogleCloudStorageHttpHandler(BUCKET);
        httpServer.createContext("/", googleCloudStorageHttpHandler);
        httpServer.createContext("/token", new FakeOAuth2HttpHandler());
        containerAndStore = createBlobContainer(randomIdentifier());
    }

    @After
    public void stopHttpServer() {
        IOUtils.closeWhileHandlingException(containerAndStore);
        httpServer.stop(0);
        ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS);
    }

    @Test
    public void testSingleMultipartWrite() throws Exception {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        final OperationPurpose purpose = randomPurpose();
        final String blobName = randomIdentifier();
        final int blobLength = randomIntBetween(1, (int) store.getLargeBlobThresholdInBytes() - 1);
        final BytesArray blobContents = new BytesArray(randomByteArrayOfLength(blobLength));
        container.writeBlob(purpose, blobName, blobContents, true);

        final StatsMap wantStats = new StatsMap(purpose);
        assertStatsEquals(wantStats.add(INSERT, 1), store.stats());
        try (InputStream is = container.readBlob(purpose, blobName)) {
            assertEquals(blobContents, Streams.readFully(is));
        }
        assertStatsEquals(wantStats.add(GET, 1), store.stats());
    }

    @Test
    public void testResumableWrite() throws Exception {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        final OperationPurpose purpose = randomPurpose();
        final String blobName = randomIdentifier();
        final int parts = between(2, 10);
        final int maxPartSize = GoogleCloudStorageBlobStore.SDK_DEFAULT_CHUNK_SIZE;
        final int size = (parts - 1) * maxPartSize + between(1, maxPartSize);
        assert size >= store.getLargeBlobThresholdInBytes();
        final BytesArray blobContents = new BytesArray(randomByteArrayOfLength(size));
        container.writeBlob(purpose, blobName, blobContents, true);

        // a resumable upload sends at least 2 requests, a POST with metadata only and multiple PUTs with SDK_DEFAULT_CHUNK_SIZE
        // the +1 means a POST request with metadata without PAYLOAD
        final int totalRequests = parts + 1;
        final StatsMap wantStats = new StatsMap(purpose);
        assertStatsEquals(wantStats.add(INSERT, 1, totalRequests), store.stats());

        try (InputStream is = container.readBlob(purpose, blobName)) {
            assertEquals(blobContents, Streams.readFully(is));
        }
        assertStatsEquals(wantStats.add(GET, 1), store.stats());
    }

    @Test
    public void testDeleteDirectory() throws Exception {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        final String directoryName = randomIdentifier();
        final BytesArray contents = new BytesArray(randomByteArrayOfLength(50));
        final int numberOfFiles = randomIntBetween(1, 20);
        final OperationPurpose purpose = randomPurpose();
        for (int i = 0; i < numberOfFiles; i++) {
            container.writeBlob(purpose, String.format("%s/file_%d", directoryName, i), contents, true);
        }
        final StatsMap wantStats = new StatsMap(purpose);
        assertStatsEquals(wantStats.add(INSERT, numberOfFiles), store.stats());

        container.delete(purpose);
        // We only count the list because we can't track the bulk delete
        assertStatsEquals(wantStats.add(LIST, 1), store.stats());
    }

    @Test
    public void testListBlobsAccountsForPaging() throws Exception {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        final int pageSize = randomIntBetween(3, 20);
        googleCloudStorageHttpHandler.setDefaultPageLimit(pageSize);
        final int numberOfPages = randomIntBetween(1, 10);
        final int numberOfObjects = randomIntBetween((numberOfPages - 1) * pageSize, numberOfPages * pageSize - 1);
        final BytesArray contents = new BytesArray(randomByteArrayOfLength(50));
        final OperationPurpose purpose = randomPurpose();
        for (int i = 0; i < numberOfObjects; i++) {
            container.writeBlob(purpose, String.format("file_%d", i), contents, true);
        }
        final StatsMap wantStats = new StatsMap(purpose);
        assertStatsEquals(wantStats.add(INSERT, numberOfObjects), store.stats());

        final Map<String, BlobMetadata> stringBlobMetadataMap = container.listBlobs(purpose);
        assertEquals(numberOfObjects, stringBlobMetadataMap.size());

        // There should be {numberOfPages} pages of blobs
        assertStatsEquals(wantStats.add(LIST, numberOfPages), store.stats());
    }

    public void testCompareAndSetRegister() {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        // update from empty (adds a single insert)
        final BytesArray contents = new BytesArray(randomByteArrayOfLength(BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH));
        final String registerName = randomIdentifier();
        final OperationPurpose purpose = randomPurpose();
        assertTrue(safeAwait(l -> container.compareAndSetRegister(purpose, registerName, BytesArray.EMPTY, contents, l)));
        final StatsMap wantStat = new StatsMap(purpose);
        assertStatsEquals(wantStat.add(GET, 1).add(INSERT, 1), store.stats());

        // successful update from non-null (adds two gets, one insert)
        final BytesArray nextContents = new BytesArray(randomByteArrayOfLength(BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH));
        assertTrue(safeAwait(l -> container.compareAndSetRegister(purpose, registerName, contents, nextContents, l)));
        assertStatsEquals(wantStat.add(GET, 2).add(INSERT, 1), store.stats());

        // failed update (adds two gets, zero inserts)
        final BytesArray wrongContents = randomValueOtherThan(
            nextContents,
            () -> new BytesArray(randomByteArrayOfLength(BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH))
        );
        assertFalse(safeAwait(l -> container.compareAndSetRegister(purpose, registerName, wrongContents, contents, l)));
        assertStatsEquals(wantStat.add(GET, 2), store.stats());
    }

    private ContainerAndBlobStore createBlobContainer(final String repositoryName) throws Exception {
        final String clientName = randomIdentifier();

        final Tuple<ServiceAccountCredentials, byte[]> serviceAccountCredentialsTuple = GoogleCloudStorageTestUtilities.randomCredential(
            clientName
        );
        final GoogleCloudStorageClientSettings clientSettings = new GoogleCloudStorageClientSettings(
            serviceAccountCredentialsTuple.v1(),
            getEndpointForServer(httpServer),
            PROJECT_ID_SETTING.getDefault(Settings.EMPTY),
            CONNECT_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            READ_TIMEOUT_SETTING.getDefault(Settings.EMPTY),
            APPLICATION_NAME_SETTING.getDefault(Settings.EMPTY),
            new URI(getEndpointForServer(httpServer) + "/token"),
            null
        );
        googleCloudStorageService.refreshAndClearCache(Map.of(clientName, clientSettings));
        final GoogleCloudStorageBlobStore blobStore = new GoogleCloudStorageBlobStore(
            BUCKET,
            clientName,
            repositoryName,
            googleCloudStorageService,
            BigArrays.NON_RECYCLING_INSTANCE,
            Math.toIntExact(BUFFER_SIZE.getBytes()),
            BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(10), 10),
            new GcsRepositoryStatsCollector()
        );
        final GoogleCloudStorageBlobContainer googleCloudStorageBlobContainer = new GoogleCloudStorageBlobContainer(
            BlobPath.EMPTY,
            blobStore
        );
        return new ContainerAndBlobStore(googleCloudStorageBlobContainer, blobStore);
    }

    protected String getEndpointForServer(final HttpServer server) {
        final InetSocketAddress address = server.getAddress();
        return "http://" + address.getHostString() + ":" + address.getPort();
    }

    private record ContainerAndBlobStore(GoogleCloudStorageBlobContainer blobContainer, GoogleCloudStorageBlobStore blobStore)
        implements
            Closeable {

        @Override
        public void close() {
            blobStore.close();
        }
    }

    class StatsMap extends HashMap<String, BlobStoreActionStats> {
        private final OperationPurpose purpose;

        StatsMap(OperationPurpose purpose) {
            this.purpose = purpose;
            for (var o : StorageOperation.values()) {
                put(o.key(), new BlobStoreActionStats(0, 0));
            }
        }

        StatsMap add(StorageOperation operation, long ops) {
            compute(operation.key(), (k, v) -> {
                assert v != null;
                return new BlobStoreActionStats(v.operations() + ops, v.requests() + ops);
            });
            return this;
        }

        StatsMap add(StorageOperation operation, long ops, long reqs) {
            compute(operation.key(), (k, v) -> {
                assert v != null;
                return new BlobStoreActionStats(v.operations() + ops, v.requests() + reqs);
            });
            return this;
        }
    }
}
