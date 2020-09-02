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

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.PlainBlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

/**
 * Mock Repository that allows testing the eventually consistent behaviour of AWS S3 as documented in the
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">AWS S3 docs</a>.
 * Currently, the repository asserts that no inconsistent reads are made.
 * TODO: Resolve todos on list and overwrite operation consistency to fully cover S3's behavior.
 */
public class MockEventuallyConsistentRepository extends BlobStoreRepository {

    private final Random random;

    private final Context context;

    private final NamedXContentRegistry namedXContentRegistry;

    public MockEventuallyConsistentRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings,
        final Context context,
        final Random random) {
        super(metadata, namedXContentRegistry, clusterService, recoverySettings, BlobPath.cleanPath());
        this.context = context;
        this.namedXContentRegistry = namedXContentRegistry;
        this.random = random;
    }

    // Filters out all actions that are super-seeded by subsequent actions
    // TODO: Remove all usages of this method, snapshots should not depend on consistent list operations
    private static List<BlobStoreAction> consistentView(List<BlobStoreAction> actions) {
        final Map<String, BlobStoreAction> lastActions = new HashMap<>();
        for (BlobStoreAction action : actions) {
            if (action.operation == Operation.PUT) {
                lastActions.put(action.path, action);
            } else if (action.operation == Operation.DELETE) {
                lastActions.remove(action.path);
            }
        }
        return List.copyOf(lastActions.values());
    }

    @Override
    protected void assertSnapshotOrGenericThread() {
        // eliminate thread name check as we create repo in the test thread
    }

    @Override
    protected BlobStore createBlobStore() {
        return new MockBlobStore();
    }

    /**
     * Context that must be shared between all instances of {@link MockEventuallyConsistentRepository} in a test run.
     */
    public static final class Context {

        // Eventual consistency is only simulated as long as this flag is false
        private boolean consistent;

        private final List<BlobStoreAction> actions = new ArrayList<>();

        /**
         * Force the repository into a consistent end state so that its eventual state can be examined.
         */
        public void forceConsistent() {
            synchronized (actions) {
                final List<BlobStoreAction> consistentActions = consistentView(actions);
                actions.clear();
                actions.addAll(consistentActions);
                consistent = true;
            }
        }
    }

    private enum Operation {
        PUT, GET, DELETE
    }

    private static final class BlobStoreAction {

        private final Operation operation;

        @Nullable
        private final byte[] data;

        private final String path;

        private BlobStoreAction(Operation operation, String path, byte[] data) {
            this.operation = operation;
            this.path = path;
            this.data = data;
        }

        private BlobStoreAction(Operation operation, String path) {
            this(operation, path, null);
        }
    }

    private class MockBlobStore implements BlobStore {

        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new MockBlobContainer(path);
        }

        @Override
        public void close() {
            closed.set(true);
        }

        private void ensureNotClosed() {
            if (closed.get()) {
                throw new AssertionError("Blobstore is closed already");
            }
        }

        private class MockBlobContainer implements BlobContainer {

            private final BlobPath path;

            MockBlobContainer(BlobPath path) {
                this.path = path;
            }

            @Override
            public BlobPath path() {
                return path;
            }

            @Override
            public boolean blobExists(String blobName) {
                try {
                    readBlob(blobName);
                    return true;
                } catch (NoSuchFileException ignored) {
                    return false;
                }
            }

            @Override
            public InputStream readBlob(String name) throws NoSuchFileException {
                ensureNotClosed();
                final String blobPath = path.buildAsString() + name;
                synchronized (context.actions) {
                    final List<BlobStoreAction> relevantActions = relevantActions(blobPath);
                    context.actions.add(new BlobStoreAction(Operation.GET, blobPath));
                    if (relevantActions.stream().noneMatch(a -> a.operation == Operation.PUT)) {
                        throw new NoSuchFileException(blobPath);
                    }
                    if (relevantActions.size() == 1 && relevantActions.get(0).operation == Operation.PUT) {
                        // Consistent read after write
                        return new ByteArrayInputStream(relevantActions.get(0).data);
                    }
                    throw new AssertionError("Inconsistent read on [" + blobPath + ']');
                }
            }

            @Override
            public InputStream readBlob(String blobName, long position, long length) throws IOException {
                final InputStream stream = readBlob(blobName);
                if (position > 0) {
                    stream.skip(position);
                }
                return Streams.limitStream(stream, length);
            }

            private List<BlobStoreAction> relevantActions(String blobPath) {
                assert Thread.holdsLock(context.actions);
                final List<BlobStoreAction> relevantActions = new ArrayList<>(
                    context.actions.stream().filter(action -> blobPath.equals(action.path)).collect(Collectors.toList()));
                for (int i = relevantActions.size() - 1; i > 0; i--) {
                    if (relevantActions.get(i).operation == Operation.GET) {
                        relevantActions.remove(i);
                    } else {
                        break;
                    }
                }
                return relevantActions;
            }

            @Override
            public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
                ensureNotClosed();
                synchronized (context.actions) {
                    for (String blobName : blobNames) {
                        context.actions.add(new BlobStoreAction(Operation.DELETE, path.buildAsString() + blobName));
                    }
                }
            }

            @Override
            public DeleteResult delete() {
                ensureNotClosed();
                final String thisPath = path.buildAsString();
                final AtomicLong bytesDeleted = new AtomicLong(0L);
                final AtomicLong blobsDeleted = new AtomicLong(0L);
                synchronized (context.actions) {
                    consistentView(context.actions).stream().filter(action -> action.path.startsWith(thisPath))
                        .forEach(a -> {
                            context.actions.add(new BlobStoreAction(Operation.DELETE, a.path));
                            bytesDeleted.addAndGet(a.data.length);
                            blobsDeleted.incrementAndGet();
                        });
                }
                return new DeleteResult(blobsDeleted.get(), bytesDeleted.get());
            }

            @Override
            public Map<String, BlobMetadata> listBlobs() {
                ensureNotClosed();
                final String thisPath = path.buildAsString();
                synchronized (context.actions) {
                    return maybeMissLatestIndexN(consistentView(context.actions).stream()
                        .filter(
                            action -> action.path.startsWith(thisPath) && action.path.substring(thisPath.length()).indexOf('/') == -1
                                && action.operation == Operation.PUT)
                        .collect(
                            Collectors.toMap(
                                action -> action.path.substring(thisPath.length()),
                                action -> new PlainBlobMetadata(action.path.substring(thisPath.length()), action.data.length))));
                }
            }

            @Override
            public Map<String, BlobContainer> children() {
                ensureNotClosed();
                final String thisPath = path.buildAsString();
                synchronized (context.actions) {
                    return consistentView(context.actions).stream()
                        .filter(action ->
                            action.operation == Operation.PUT
                                && action.path.startsWith(thisPath) && action.path.substring(thisPath.length()).indexOf('/') != -1)
                        .map(action -> action.path.substring(thisPath.length()).split("/")[0])
                        .distinct()
                        .collect(Collectors.toMap(Function.identity(), name -> new MockBlobContainer(path.add(name))));
                }
            }

            @Override
            public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
                return maybeMissLatestIndexN(
                    Maps.ofEntries(listBlobs().entrySet().stream().filter(entry -> entry.getKey().startsWith(blobNamePrefix))
                        .collect(Collectors.toList())));
            }

            // Randomly filter out the index-N blobs from a listing to test that tracking of it in latestKnownRepoGen and the cluster state
            // ensures consistent repository operations
            private Map<String, BlobMetadata> maybeMissLatestIndexN(Map<String, BlobMetadata> listing) {
                // Randomly filter out index-N blobs at the repo root to proof that we don't need them to be consistently listed
                if (path.parent() == null && context.consistent == false) {
                    final Map<String, BlobMetadata> filtered = new HashMap<>(listing);
                    filtered.keySet().removeIf(b -> b.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX) && random.nextBoolean());
                    return Map.copyOf(filtered);
                }
                return listing;
            }

            @Override
            public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                    throws IOException {
                ensureNotClosed();
                assert blobSize < Integer.MAX_VALUE;
                final byte[] data = new byte[(int) blobSize];
                final int read = inputStream.read(data);
                assert read == data.length;
                final String blobPath = path.buildAsString() + blobName;
                synchronized (context.actions) {
                    final List<BlobStoreAction> relevantActions = relevantActions(blobPath);
                    // We do some checks in case there is a consistent state for a blob to prevent turning it inconsistent.
                    final boolean hasConsistentContent =
                        relevantActions.size() == 1 && relevantActions.get(0).operation == Operation.PUT;
                    if (BlobStoreRepository.INDEX_LATEST_BLOB.equals(blobName)
                        || blobName.startsWith(BlobStoreRepository.METADATA_PREFIX)) {
                        // TODO: Ensure that it is impossible to ever decrement the generation id stored in index.latest then assert that
                        //       it never decrements here. Same goes for the metadata, ensure that we never overwrite newer with older
                        //       metadata.
                    } else if (blobName.startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)) {
                        if (hasConsistentContent) {
                                if (basePath().buildAsString().equals(path().buildAsString())) {
                                    try {
                                        final SnapshotInfo updatedInfo = BlobStoreRepository.SNAPSHOT_FORMAT.deserialize(
                                                blobName, namedXContentRegistry, new BytesArray(data));
                                        // If the existing snapshotInfo differs only in the timestamps it stores, then the overwrite is not
                                        // a problem and could be the result of a correctly handled master failover.
                                        final SnapshotInfo existingInfo = SNAPSHOT_FORMAT.deserialize(
                                                blobName, namedXContentRegistry, Streams.readFully(readBlob(blobName)));
                                        assertThat(existingInfo.snapshotId(), equalTo(updatedInfo.snapshotId()));
                                        assertThat(existingInfo.reason(), equalTo(updatedInfo.reason()));
                                        assertThat(existingInfo.state(), equalTo(updatedInfo.state()));
                                        assertThat(existingInfo.totalShards(), equalTo(updatedInfo.totalShards()));
                                        assertThat(existingInfo.successfulShards(), equalTo(updatedInfo.successfulShards()));
                                        assertThat(
                                            existingInfo.shardFailures(), containsInAnyOrder(updatedInfo.shardFailures().toArray()));
                                        assertThat(existingInfo.indices(), equalTo(updatedInfo.indices()));
                                        return; // No need to add a write for this since we didn't change content
                                    } catch (Exception e) {
                                        // Rethrow as AssertionError here since kind exception might otherwise be swallowed and logged by
                                        // the blob store repository.
                                        // Since we are not doing any actual IO we don't expect this to throw ever and an exception would
                                        // signal broken SnapshotInfo bytes or unexpected behavior of SnapshotInfo otherwise.
                                        throw new AssertionError("Failed to deserialize SnapshotInfo", e);
                                    }
                                } else {
                                    // Primaries never retry so any shard level snap- blob retry/overwrite even with the same content is
                                    // not expected.
                                    throw new AssertionError("Shard level snap-{uuid} blobs should never be overwritten");
                                }
                        }
                    } else {
                        if (hasConsistentContent) {
                            ESTestCase.assertArrayEquals("Tried to overwrite blob [" + blobName + "]", relevantActions.get(0).data, data);
                            return; // No need to add a write for this since we didn't change content
                        }
                    }
                    context.actions.add(new BlobStoreAction(Operation.PUT, blobPath, data));
                }
            }

            @Override
            public void writeBlobAtomic(final String blobName, final InputStream inputStream, final long blobSize,
                final boolean failIfAlreadyExists) throws IOException {
                writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        }
    }
}
