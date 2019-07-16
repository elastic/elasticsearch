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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Mock Repository that allows testing the eventually consistent behaviour of AWS S3 as documented in the
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel">AWS S3 docs</a>.
 * Currently, the repository asserts that no inconsistent reads are made.
 * TODO: Resolve todos on list and overwrite operation consistency to fully cover S3's behavior.
 */
public class MockEventuallyConsistentRepository extends BlobStoreRepository {

    private final Context context;

    public MockEventuallyConsistentRepository(RepositoryMetaData metadata, Environment environment,
        NamedXContentRegistry namedXContentRegistry, ThreadPool threadPool, Context context) {
        super(metadata, environment.settings(), namedXContentRegistry, threadPool, BlobPath.cleanPath());
        this.context = context;
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

        private final List<BlobStoreAction> actions = new ArrayList<>();

        /**
         * Force the repository into a consistent end state so that its eventual state can be examined.
         */
        public void forceConsistent() {
            synchronized (actions) {
                final List<BlobStoreAction> consistentActions = consistentView(actions);
                actions.clear();
                actions.addAll(consistentActions);
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

        private AtomicBoolean closed = new AtomicBoolean(false);

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
                ensureNotClosed();
                try {
                    readBlob(blobName);
                    return true;
                } catch (NoSuchFileException e) {
                    return false;
                }
            }

            @Override
            public InputStream readBlob(String name) throws NoSuchFileException {
                ensureNotClosed();
                final String blobPath = path.buildAsString() + name;
                synchronized (context.actions) {
                    final List<BlobStoreAction> relevantActions = new ArrayList<>(
                            context.actions.stream().filter(action -> blobPath.equals(action.path)).collect(Collectors.toList()));
                    context.actions.add(new BlobStoreAction(Operation.GET, blobPath));
                    for (int i = relevantActions.size() - 1; i > 0; i--) {
                        if (relevantActions.get(i).operation == Operation.GET) {
                            relevantActions.remove(i);
                        } else {
                            break;
                        }
                    }
                    final List<byte[]> writes = new ArrayList<>();
                    boolean readBeforeWrite = false;
                    for (BlobStoreAction relevantAction : relevantActions) {
                        if (relevantAction.operation == Operation.PUT) {
                            writes.add(relevantAction.data);
                        }
                        if (writes.isEmpty() && relevantAction.operation == Operation.GET) {
                            readBeforeWrite = true;
                        }
                    }
                    if (writes.isEmpty()) {
                        throw new NoSuchFileException(blobPath);
                    }
                    if (readBeforeWrite == false && relevantActions.size() == 1) {
                        // Consistent read after write
                        return new ByteArrayInputStream(writes.get(0));
                    }
                    throw new AssertionError("Inconsistent read on [" + blobPath + ']');
                }
            }

            @Override
            public void deleteBlob(String blobName) {
                ensureNotClosed();
                synchronized (context.actions) {
                    context.actions.add(new BlobStoreAction(Operation.DELETE, path.buildAsString() + blobName));
                }
            }

            @Override
            public void delete() {
                ensureNotClosed();
                final String thisPath = path.buildAsString();
                synchronized (context.actions) {
                    consistentView(context.actions).stream().filter(action -> action.path.startsWith(thisPath))
                        .forEach(a -> context.actions.add(new BlobStoreAction(Operation.DELETE, a.path)));
                }
            }

            @Override
            public Map<String, BlobMetaData> listBlobs() {
                ensureNotClosed();
                final String thisPath = path.buildAsString();
                synchronized (context.actions) {
                    return consistentView(context.actions).stream()
                        .filter(
                            action -> action.path.startsWith(thisPath) && action.path.substring(thisPath.length()).indexOf('/') == -1
                                && action.operation == Operation.PUT)
                        .collect(
                            Collectors.toMap(
                                action -> action.path.substring(thisPath.length()),
                                action -> new PlainBlobMetaData(action.path.substring(thisPath.length()), action.data.length)));
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
            public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) {
                return Maps.ofEntries(
                    listBlobs().entrySet().stream().filter(entry -> entry.getKey().startsWith(blobNamePrefix)).collect(Collectors.toList())
                );
            }

            @Override
            public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
                throws IOException {
                ensureNotClosed();
                // TODO: Throw if we try to overwrite any blob other than incompatible_snapshots or index.latest with different content
                //       than it already contains.
                assert blobSize < Integer.MAX_VALUE;
                final byte[] data = new byte[(int) blobSize];
                final int read = inputStream.read(data);
                assert read == data.length;
                synchronized (context.actions) {
                    context.actions.add(new BlobStoreAction(Operation.PUT, path.buildAsString() + blobName, data));
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
