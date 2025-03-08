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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.repositories.blobstore.BlobStoreTestUtil.randomPurpose;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.APPLICATION_NAME_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.CONNECT_TIMEOUT_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.PROJECT_ID_SETTING;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageClientSettings.READ_TIMEOUT_SETTING;

@SuppressForbidden(reason = "Uses a HttpServer to emulate a Google Cloud Storage endpoint")
public class GoogleCloudStorageBlobContainerStatsTests extends ESTestCase {
    private static final String BUCKET = "bucket";
    private static final ByteSizeValue BUFFER_SIZE = ByteSizeValue.ofKb(128);

    private HttpServer httpServer;
    private ThreadPool threadPool;
    private GoogleCloudStorageService googleCloudStorageService;
    private GoogleCloudStorageHttpHandler googleCloudStorageHttpHandler;
    private ContainerAndBlobStore containerAndStore;

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

        final String blobName = randomIdentifier();
        final int blobLength = randomIntBetween(1, (int) store.getLargeBlobThresholdInBytes() - 1);
        final BytesArray blobContents = new BytesArray(randomByteArrayOfLength(blobLength));
        container.writeBlob(randomPurpose(), blobName, blobContents, true);
        assertEquals(createStats(1, 0, 0), store.stats());

        try (InputStream is = container.readBlob(randomPurpose(), blobName)) {
            assertEquals(blobContents, Streams.readFully(is));
        }
        assertEquals(createStats(1, 0, 1), store.stats());
    }

    @Test
    public void testResumableWrite() throws Exception {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        final String blobName = randomIdentifier();
        final int size = randomIntBetween((int) store.getLargeBlobThresholdInBytes(), (int) store.getLargeBlobThresholdInBytes() * 2);
        final BytesArray blobContents = new BytesArray(randomByteArrayOfLength(size));
        container.writeBlob(randomPurpose(), blobName, blobContents, true);
        assertEquals(createStats(1, 0, 0), store.stats());

        try (InputStream is = container.readBlob(randomPurpose(), blobName)) {
            assertEquals(blobContents, Streams.readFully(is));
        }
        assertEquals(createStats(1, 0, 1), store.stats());
    }

    @Test
    public void testDeleteDirectory() throws Exception {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        final String directoryName = randomIdentifier();
        final BytesArray contents = new BytesArray(randomByteArrayOfLength(50));
        final int numberOfFiles = randomIntBetween(1, 20);
        for (int i = 0; i < numberOfFiles; i++) {
            container.writeBlob(randomPurpose(), String.format("%s/file_%d", directoryName, i), contents, true);
        }
        assertEquals(createStats(numberOfFiles, 0, 0), store.stats());

        container.delete(randomPurpose());
        // We only count the list because we can't track the bulk delete
        assertEquals(createStats(numberOfFiles, 1, 0), store.stats());
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
        for (int i = 0; i < numberOfObjects; i++) {
            container.writeBlob(randomPurpose(), String.format("file_%d", i), contents, true);
        }
        assertEquals(createStats(numberOfObjects, 0, 0), store.stats());

        final Map<String, BlobMetadata> stringBlobMetadataMap = container.listBlobs(randomPurpose());
        assertEquals(numberOfObjects, stringBlobMetadataMap.size());
        // There should be {numberOfPages} pages of blobs
        assertEquals(createStats(numberOfObjects, numberOfPages, 0), store.stats());
    }

    public void testCompareAndSetRegister() {
        final GoogleCloudStorageBlobContainer container = containerAndStore.blobContainer();
        final GoogleCloudStorageBlobStore store = containerAndStore.blobStore();

        // update from empty (adds a single insert)
        final BytesArray contents = new BytesArray(randomByteArrayOfLength(BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH));
        final String registerName = randomIdentifier();
        assertTrue(safeAwait(l -> container.compareAndSetRegister(randomPurpose(), registerName, BytesArray.EMPTY, contents, l)));
        assertEquals(createStats(1, 0, 0), store.stats());

        // successful update from non-null (adds two gets, one insert)
        final BytesArray nextContents = new BytesArray(randomByteArrayOfLength(BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH));
        assertTrue(safeAwait(l -> container.compareAndSetRegister(randomPurpose(), registerName, contents, nextContents, l)));
        assertEquals(createStats(2, 0, 2), store.stats());

        // failed update (adds two gets, zero inserts)
        final BytesArray wrongContents = randomValueOtherThan(
            nextContents,
            () -> new BytesArray(randomByteArrayOfLength(BlobContainerUtils.MAX_REGISTER_CONTENT_LENGTH))
        );
        assertFalse(safeAwait(l -> container.compareAndSetRegister(randomPurpose(), registerName, wrongContents, contents, l)));
        assertEquals(createStats(2, 0, 4), store.stats());
    }

    private Map<String, BlobStoreActionStats> createStats(int insertCount, int listCount, int getCount) {
        return Map.of(
            "GetObject",
            new BlobStoreActionStats(getCount, getCount),
            "ListObjects",
            new BlobStoreActionStats(listCount, listCount),
            "InsertObject",
            new BlobStoreActionStats(insertCount, insertCount)
        );
    }

    private record ContainerAndBlobStore(GoogleCloudStorageBlobContainer blobContainer, GoogleCloudStorageBlobStore blobStore)
        implements
            Closeable {

        @Override
        public void close() {
            blobStore.close();
        }
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
            BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(10), 10)
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
}
