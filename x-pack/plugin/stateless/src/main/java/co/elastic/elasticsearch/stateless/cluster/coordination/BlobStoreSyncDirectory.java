/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

class BlobStoreSyncDirectory extends FilterDirectory {
    private static final Comparator<Tuple<String, Long>> FILES_SIZE_COMPARATOR = Comparator.<Tuple<String, Long>>comparingLong(Tuple::v2)
        .reversed();
    private final Logger logger = LogManager.getLogger(BlobStoreSyncDirectory.class);

    private final Supplier<BlobContainer> blobContainerSupplier;
    private final ThrottledTaskRunner taskRunner;
    private final Set<String> filesToDelete = new HashSet<>();
    private final Set<String> uploadedFiles = Collections.synchronizedSet(new HashSet<>());
    private final AtomicBoolean closed = new AtomicBoolean();
    private SegmentInfos latestCommit;

    BlobStoreSyncDirectory(Directory delegate, Supplier<BlobContainer> blobContainerSupplier, Executor executorService) {
        super(delegate);
        this.blobContainerSupplier = blobContainerSupplier;
        this.taskRunner = new ThrottledTaskRunner("cluster_state_io_runner", 5, executorService);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (uploadedFiles.remove(name)) {
            synchronized (this) {
                filesToDelete.add(name);
            }
        }

        super.deleteFile(name);
    }

    @Override
    public void sync(Collection<String> fileNames) throws IOException {
        var filesToUpload = getFilesToUploadOrderedBySize(fileNames);
        if (filesToUpload.isEmpty()) {
            return;
        }

        PlainActionFuture<Void> allDownloadsFinished = new PlainActionFuture<>();
        try (var listener = new RefCountingListener(allDownloadsFinished)) {
            for (String fileToUpload : filesToUpload) {
                taskRunner.enqueueTask(listener.acquire().map(r -> {
                    try (r) {
                        if (closed.get()) {
                            return null;
                        }

                        var blobContainer = blobContainerSupplier.get();
                        writeFile(fileToUpload, blobContainer, false);
                    }
                    return null;
                }));
            }
        }
        // TODO: Check closed flag on each IndexInput and hold #close until all activity is finished
        FutureUtils.get(allDownloadsFinished);
    }

    @Override
    public void syncMetaData() {
        // noop, data on local drive need not be safely persisted
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        assert source.startsWith(IndexFileNames.PENDING_SEGMENTS) : source;
        assert dest.startsWith(IndexFileNames.SEGMENTS) : dest;

        in.rename(source, dest);

        final PlainActionFuture<SegmentInfos> uploadSegmentsFileFuture = new PlainActionFuture<>();
        taskRunner.enqueueTask(uploadSegmentsFileFuture.map(r -> {
            try (r) {
                final var blobContainer = blobContainerSupplier.get();
                writeFile(dest, blobContainer, true);
                scheduleDeleteUnusedFiles();
                return SegmentInfos.readCommit(BlobStoreSyncDirectory.this, dest);
            }
        }));
        // TODO: Check if the directory has been closed in the dispatched task
        this.latestCommit = FutureUtils.get(uploadSegmentsFileFuture);
    }

    private void scheduleDeleteUnusedFiles() {
        final Set<String> filesToDeleteInTask = getFilesToDelete();
        if (filesToDeleteInTask.isEmpty() == false) {
            // TODO: maybe throttle deletions differently and maybe batch deletions between commits
            final BlobContainer blobContainer = blobContainerSupplier.get();
            taskRunner.enqueueTask(new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try (releasable) {
                        if (closed.get()) {
                            return;
                        }
                        blobContainer.deleteBlobsIgnoringIfNotExists(OperationPurpose.CLUSTER_STATE, filesToDeleteInTask.iterator());
                        onFilesDeleted(filesToDeleteInTask);
                    } catch (IOException e) {
                        logger.debug("Unable to delete cluster state stale files {}", filesToDeleteInTask);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.debug("Unable to delete cluster state stale files {}", filesToDeleteInTask);
                }
            });
        }
    }

    private synchronized Set<String> getFilesToDelete() {
        return new HashSet<>(filesToDelete);
    }

    private synchronized void onFilesDeleted(Set<String> deletedFiles) {
        filesToDelete.removeAll(deletedFiles);
    }

    @Override
    public void close() throws IOException {
        super.close();
        closed.set(true);
    }

    private void writeFile(String fileName, BlobContainer blobContainer, boolean atomic) throws IOException {
        uploadedFiles.add(fileName);
        try (var input = openInput(fileName, IOContext.READONCE)) {
            final long length = input.length();
            final InputStream inputStream = new InputStreamIndexInput(input, length);
            if (atomic) {
                blobContainer.writeMetadataBlob(
                    OperationPurpose.CLUSTER_STATE,
                    fileName,
                    false,
                    true,
                    out -> Streams.copy(inputStream, out, false)
                );
            } else {
                blobContainer.writeBlob(OperationPurpose.CLUSTER_STATE, fileName, inputStream, length, false);
            }
        }
    }

    private Collection<String> getFilesToUploadOrderedBySize(Collection<String> fileNames) {
        final Collection<String> filesToUpload;
        if (latestCommit == null) {
            filesToUpload = fileNames;
        } else {
            try {
                filesToUpload = new HashSet<>(fileNames);
                filesToUpload.removeAll(latestCommit.files(false));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return filesToUpload.stream().filter(fileName -> fileName.startsWith(IndexFileNames.PENDING_SEGMENTS) == false).map(file -> {
            try {
                return Tuple.tuple(file, fileLength(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).sorted(FILES_SIZE_COMPARATOR).map(Tuple::v1).toList();
    }
}
