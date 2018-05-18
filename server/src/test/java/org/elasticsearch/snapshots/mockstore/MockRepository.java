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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;

public class MockRepository extends FsRepository {

    public static class Plugin extends org.elasticsearch.plugins.Plugin implements RepositoryPlugin {

        public static final Setting<String> USERNAME_SETTING = Setting.simpleString("secret.mock.username", Property.NodeScope);
        public static final Setting<String> PASSWORD_SETTING =
            Setting.simpleString("secret.mock.password", Property.NodeScope, Property.Filtered);


        @Override
        public Map<String, Repository.Factory> getRepositories(Environment env, NamedXContentRegistry namedXContentRegistry) {
            return Collections.singletonMap("mock", (metadata) -> new MockRepository(metadata, env, namedXContentRegistry));
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(USERNAME_SETTING, PASSWORD_SETTING);
        }
    }

    private final AtomicLong failureCounter = new AtomicLong();

    public long getFailureCount() {
        return failureCounter.get();
    }

    private final double randomControlIOExceptionRate;

    private final double randomDataFileIOExceptionRate;

    private final boolean useLuceneCorruptionException;

    private final long maximumNumberOfFailures;

    private final long waitAfterUnblock;

    private final MockBlobStore mockBlobStore;

    private final String randomPrefix;

    private volatile boolean blockOnInitialization;

    private volatile boolean blockOnControlFiles;

    private volatile boolean blockOnDataFiles;

    /** Allows blocking on writing the index-N blob; this is a way to enforce blocking the
     *  finalization of a snapshot, while permitting other IO operations to proceed unblocked. */
    private volatile boolean blockOnWriteIndexFile;

    /** Allows blocking on writing the snapshot file at the end of snapshot creation to simulate a died master node */
    private volatile boolean blockAndFailOnWriteSnapFile;

    private volatile boolean atomicMove;

    private volatile boolean blocked = false;

    public MockRepository(RepositoryMetaData metadata, Environment environment,
                          NamedXContentRegistry namedXContentRegistry) throws IOException {
        super(overrideSettings(metadata, environment), environment, namedXContentRegistry);
        randomControlIOExceptionRate = metadata.settings().getAsDouble("random_control_io_exception_rate", 0.0);
        randomDataFileIOExceptionRate = metadata.settings().getAsDouble("random_data_file_io_exception_rate", 0.0);
        useLuceneCorruptionException = metadata.settings().getAsBoolean("use_lucene_corruption", false);
        maximumNumberOfFailures = metadata.settings().getAsLong("max_failure_number", 100L);
        blockOnControlFiles = metadata.settings().getAsBoolean("block_on_control", false);
        blockOnDataFiles = metadata.settings().getAsBoolean("block_on_data", false);
        blockOnInitialization = metadata.settings().getAsBoolean("block_on_init", false);
        blockAndFailOnWriteSnapFile = metadata.settings().getAsBoolean("block_on_snap", false);
        randomPrefix = metadata.settings().get("random", "default");
        waitAfterUnblock = metadata.settings().getAsLong("wait_after_unblock", 0L);
        atomicMove = metadata.settings().getAsBoolean("atomic_move", true);
        logger.info("starting mock repository with random prefix {}", randomPrefix);
        mockBlobStore = new MockBlobStore(super.blobStore());
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData clusterMetadata) {
        if (blockOnInitialization) {
            blockExecution();
        }
        super.initializeSnapshot(snapshotId, indices, clusterMetadata);
    }

    private static RepositoryMetaData overrideSettings(RepositoryMetaData metadata, Environment environment) {
        // TODO: use another method of testing not being able to read the test file written by the master...
        // this is super duper hacky
        if (metadata.settings().getAsBoolean("localize_location", false)) {
            Path location = PathUtils.get(metadata.settings().get("location"));
            location = location.resolve(Integer.toString(environment.hashCode()));
            return new RepositoryMetaData(metadata.name(), metadata.type(),
                Settings.builder().put(metadata.settings()).put("location", location.toAbsolutePath()).build());
        } else {
            return metadata;
        }
    }

    private long incrementAndGetFailureCount() {
        return failureCounter.incrementAndGet();
    }

    @Override
    protected void doStop() {
        unblock();
        super.doStop();
    }

    @Override
    protected BlobStore blobStore() {
        return mockBlobStore;
    }

    public synchronized void unblock() {
        blocked = false;
        // Clean blocking flags, so we wouldn't try to block again
        blockOnDataFiles = false;
        blockOnControlFiles = false;
        blockOnInitialization = false;
        blockOnWriteIndexFile = false;
        blockAndFailOnWriteSnapFile = false;
        this.notifyAll();
    }

    public void blockOnDataFiles(boolean blocked) {
        blockOnDataFiles = blocked;
    }

    public void setBlockAndFailOnWriteSnapFiles(boolean blocked) {
        blockAndFailOnWriteSnapFile = blocked;
    }

    public void setBlockOnWriteIndexFile(boolean blocked) {
        blockOnWriteIndexFile = blocked;
    }

    public boolean blocked() {
        return blocked;
    }

    private synchronized boolean blockExecution() {
        logger.debug("Blocking execution");
        boolean wasBlocked = false;
        try {
            while (blockOnDataFiles || blockOnControlFiles || blockOnInitialization || blockOnWriteIndexFile ||
                blockAndFailOnWriteSnapFile) {
                blocked = true;
                this.wait();
                wasBlocked = true;
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        logger.debug("Unblocking execution");
        return wasBlocked;
    }

    public class MockBlobStore extends BlobStoreWrapper {
        ConcurrentMap<String, AtomicLong> accessCounts = new ConcurrentHashMap<>();

        private long incrementAndGet(String path) {
            AtomicLong value = accessCounts.get(path);
            if (value == null) {
                value = accessCounts.putIfAbsent(path, new AtomicLong(1));
            }
            if (value != null) {
                return value.incrementAndGet();
            }
            return 1;
        }

        public MockBlobStore(BlobStore delegate) {
            super(delegate);
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new MockBlobContainer(super.blobContainer(path));
        }

        private class MockBlobContainer extends BlobContainerWrapper {
            private MessageDigest digest;

            private boolean shouldFail(String blobName, double probability) {
                if (probability > 0.0) {
                    String path = path().add(blobName).buildAsString() + randomPrefix;
                    path += "/" + incrementAndGet(path);
                    logger.info("checking [{}] [{}]", path, Math.abs(hashCode(path)) < Integer.MAX_VALUE * probability);
                    return Math.abs(hashCode(path)) < Integer.MAX_VALUE * probability;
                } else {
                    return false;
                }
            }

            private int hashCode(String path) {
                try {
                    digest = MessageDigest.getInstance("MD5");
                    byte[] bytes = digest.digest(path.getBytes("UTF-8"));
                    int i = 0;
                    return ((bytes[i++] & 0xFF) << 24) | ((bytes[i++] & 0xFF) << 16)
                            | ((bytes[i++] & 0xFF) << 8) | (bytes[i++] & 0xFF);
                } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
                    throw new ElasticsearchException("cannot calculate hashcode", ex);
                }
            }

            private void maybeIOExceptionOrBlock(String blobName) throws IOException {
                if (blobName.startsWith("__")) {
                    if (shouldFail(blobName, randomDataFileIOExceptionRate) && (incrementAndGetFailureCount() < maximumNumberOfFailures)) {
                        logger.info("throwing random IOException for file [{}] at path [{}]", blobName, path());
                        if (useLuceneCorruptionException) {
                            throw new CorruptIndexException("Random corruption", "random file");
                        } else {
                            throw new IOException("Random IOException");
                        }
                    } else if (blockOnDataFiles) {
                        blockExecutionAndMaybeWait(blobName);
                    }
                } else {
                    if (shouldFail(blobName, randomControlIOExceptionRate) && (incrementAndGetFailureCount() < maximumNumberOfFailures)) {
                        logger.info("throwing random IOException for file [{}] at path [{}]", blobName, path());
                        throw new IOException("Random IOException");
                    } else if (blockOnControlFiles) {
                        blockExecutionAndMaybeWait(blobName);
                    } else if (blobName.startsWith("snap-") && blockAndFailOnWriteSnapFile) {
                        blockExecutionAndFail(blobName);
                    }
                }
            }

            private void blockExecutionAndMaybeWait(final String blobName) {
                logger.info("blocking I/O operation for file [{}] at path [{}]", blobName, path());
                if (blockExecution() && waitAfterUnblock > 0) {
                    try {
                        // Delay operation after unblocking
                        // So, we can start node shutdown while this operation is still running.
                        Thread.sleep(waitAfterUnblock);
                    } catch (InterruptedException ex) {
                        //
                    }
                }
            }

            /**
             * Blocks an I/O operation on the blob fails and throws an exception when unblocked
             */
            private void blockExecutionAndFail(final String blobName) throws IOException {
                logger.info("blocking I/O operation for file [{}] at path [{}]", blobName, path());
                blockExecution();
                throw new IOException("exception after block");
            }

            MockBlobContainer(BlobContainer delegate) {
                super(delegate);
            }

            @Override
            public boolean blobExists(String blobName) {
                return super.blobExists(blobName);
            }

            @Override
            public InputStream readBlob(String name) throws IOException {
                maybeIOExceptionOrBlock(name);
                return super.readBlob(name);
            }

            @Override
            public void deleteBlob(String blobName) throws IOException {
                maybeIOExceptionOrBlock(blobName);
                super.deleteBlob(blobName);
            }

            @Override
            public Map<String, BlobMetaData> listBlobs() throws IOException {
                maybeIOExceptionOrBlock("");
                return super.listBlobs();
            }

            @Override
            public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
                maybeIOExceptionOrBlock(blobNamePrefix);
                return super.listBlobsByPrefix(blobNamePrefix);
            }

            @Override
            public void move(String sourceBlob, String targetBlob) throws IOException {
                if (blockOnWriteIndexFile && targetBlob.startsWith("index-")) {
                    blockExecutionAndMaybeWait(targetBlob);
                }
                if (atomicMove) {
                    // atomic move since this inherits from FsBlobContainer which provides atomic moves
                    maybeIOExceptionOrBlock(targetBlob);
                    super.move(sourceBlob, targetBlob);
                } else {
                    // simulate a non-atomic move, since many blob container implementations
                    // will not have an atomic move, and we should be able to handle that
                    maybeIOExceptionOrBlock(targetBlob);
                    super.writeBlob(targetBlob, super.readBlob(sourceBlob), 0L);
                    super.deleteBlob(sourceBlob);
                }
            }

            @Override
            public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
                maybeIOExceptionOrBlock(blobName);
                super.writeBlob(blobName, inputStream, blobSize);
                if (RandomizedContext.current().getRandom().nextBoolean()) {
                    // for network based repositories, the blob may have been written but we may still
                    // get an error with the client connection, so an IOException here simulates this
                    maybeIOExceptionOrBlock(blobName);
                }
            }
        }
    }
}
