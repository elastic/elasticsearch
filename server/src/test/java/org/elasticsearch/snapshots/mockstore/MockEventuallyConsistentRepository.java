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

import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.fs.FsRepository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Mock Repository that simulates the mechanics of an eventually consistent blobstore.
 */
public class MockEventuallyConsistentRepository extends FsRepository {

    private final DeterministicTaskQueue deterministicTaskQueue;

    private final Context context;

    public MockEventuallyConsistentRepository(RepositoryMetaData metadata, Environment environment,
                          NamedXContentRegistry namedXContentRegistry, DeterministicTaskQueue deterministicTaskQueue, Context context) {
        super(metadata, environment, namedXContentRegistry, deterministicTaskQueue.getThreadPool());
        this.deterministicTaskQueue = deterministicTaskQueue;
        this.context = context;
    }

    @Override
    protected void assertSnapshotOrGenericThread() {
        // eliminate thread name check as we create repo in the test thread
    }

    @Override
    protected BlobStore createBlobStore() throws Exception {
        return new MockBlobStore(super.createBlobStore());
    }

    public static final class Context {

        private final Map<BlobPath, Tuple<Set<String>, Map<String, Runnable>>> state = new HashMap<>();

        public Tuple<Set<String>, Map<String, Runnable>> getState(BlobPath path) {
            return state.computeIfAbsent(path, p -> new Tuple<>(new HashSet<>(), new HashMap<>()));
        }

    }

    private class MockBlobStore extends BlobStoreWrapper {

        MockBlobStore(BlobStore delegate) {
            super(delegate);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new MockBlobContainer(super.blobContainer(path), context.getState(path));
        }

        private class MockBlobContainer extends BlobContainerWrapper {

            private final Set<String> cachedMisses;

            private final Map<String, Runnable> pendingWrites;

            MockBlobContainer(BlobContainer delegate, Tuple<Set<String>, Map<String, Runnable>> state) {
                super(delegate);
                cachedMisses = state.v1();
                pendingWrites = state.v2();
            }

            @Override
            public boolean blobExists(String blobName) {
                ensureReadAfterWrite(blobName);
                final boolean result = super.blobExists(blobName);
                if (result == false) {
                    cachedMisses.add(blobName);
                }
                return result;
            }

            @Override
            public InputStream readBlob(String name) throws IOException {
                ensureReadAfterWrite(name);
                return super.readBlob(name);
            }

            private void ensureReadAfterWrite(String blobName) {
                if (cachedMisses.contains(blobName) == false && pendingWrites.containsKey(blobName)) {
                    pendingWrites.remove(blobName).run();
                }
            }

            @Override
            public void deleteBlob(String blobName) {
                deterministicTaskQueue.scheduleNow(() -> {
                    try {
                        super.deleteBlob(blobName);
                    } catch (DirectoryNotEmptyException | NoSuchFileException e) {
                        // ignored
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
            }

            @Override
            public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                    throws IOException {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                Streams.copy(inputStream, baos);
                pendingWrites.put(blobName, () -> {
                    try {
                        super.writeBlob(blobName, new ByteArrayInputStream(baos.toByteArray()), blobSize, failIfAlreadyExists);
                        if (cachedMisses.contains(blobName)) {
                            deterministicTaskQueue.scheduleNow(() -> cachedMisses.remove(blobName));
                        }
                    } catch (NoSuchFileException | FileAlreadyExistsException e) {
                        // Ignoring, assuming a previous concurrent delete removed the parent path and that overwrites are not
                        // detectable with this kind of store
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                });
                deterministicTaskQueue.scheduleNow(() -> {
                    if (pendingWrites.containsKey(blobName)) {
                        pendingWrites.remove(blobName).run();
                    }
                });
            }

            @Override
            public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize,
                                        final boolean failIfAlreadyExists) throws IOException {
                writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        }
    }
}
