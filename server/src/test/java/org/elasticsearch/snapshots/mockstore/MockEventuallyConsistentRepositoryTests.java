/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.snapshots.mockstore;

import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.elasticsearch.env.Environment.PATH_HOME_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class MockEventuallyConsistentRepositoryTests extends ESTestCase {

    private Environment environment;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final Path tempDir = createTempDir();
        final String nodeName = "testNode";
        environment = TestEnvironment.newEnvironment(Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), nodeName)
            .put(PATH_HOME_SETTING.getKey(), tempDir.resolve(nodeName).toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), tempDir.resolve("repo").toAbsolutePath())
            .build());
    }

    public void testReadAfterWriteConsistently() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetaData("testRepo", "mockEventuallyConsistent", Settings.EMPTY), environment,
            xContentRegistry(), mock(ThreadPool.class), blobStoreContext)) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            try (InputStream in = blobContainer.readBlob(blobName)) {
                final byte[] readBytes = new byte[lengthWritten + 1];
                final int lengthSeen = in.read(readBytes);
                assertThat(lengthSeen, equalTo(lengthWritten));
                assertArrayEquals(blobData, Arrays.copyOf(readBytes, lengthWritten));
            }
        }
    }

    public void testReadAfterWriteAfterReadThrows() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetaData("testRepo", "mockEventuallyConsistent", Settings.EMPTY), environment,
            xContentRegistry(), mock(ThreadPool.class), blobStoreContext)) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            assertMissing(blobContainer, blobName);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            assertThrowsOnInconsistentRead(blobContainer, blobName);
        }
    }

    public void testReadAfterDeleteAfterWriteThrows() throws IOException {
        MockEventuallyConsistentRepository.Context blobStoreContext = new MockEventuallyConsistentRepository.Context();
        try (BlobStoreRepository repository = new MockEventuallyConsistentRepository(
            new RepositoryMetaData("testRepo", "mockEventuallyConsistent", Settings.EMPTY), environment,
            xContentRegistry(), mock(ThreadPool.class), blobStoreContext)) {
            repository.start();
            final BlobContainer blobContainer = repository.blobStore().blobContainer(repository.basePath());
            final String blobName = randomAlphaOfLength(10);
            final int lengthWritten = randomIntBetween(1, 100);
            final byte[] blobData = randomByteArrayOfLength(lengthWritten);
            blobContainer.writeBlob(blobName, new ByteArrayInputStream(blobData), lengthWritten, true);
            blobContainer.deleteBlob(blobName);
            assertThrowsOnInconsistentRead(blobContainer, blobName);
            blobStoreContext.forceConsistent();
            assertMissing(blobContainer, blobName);
        }
    }

    private static void assertThrowsOnInconsistentRead(BlobContainer blobContainer, String blobName) throws IOException {
        try (InputStream in = blobContainer.readBlob(blobName)) {
            fail("Inconsistent read should throw");
        } catch (AssertionError assertionError) {
            assertThat(assertionError.getMessage(), equalTo("Inconsistent read on [" + blobName + ']'));
        }
    }

    private static void assertMissing(BlobContainer container, String blobName) throws IOException {
        try (InputStream in = container.readBlob(blobName)) {
            fail("Reading a non-existent blob should throw");
        } catch (NoSuchFileException expected) {
        }
    }
}
