/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class StatelessCommitService {

    // We don't do null checks when reading from this sub-map because we hold a commit reference while files are being uploaded. This will
    // prevent commit deletion in the interim.
    private final ConcurrentHashMap<ShardId, ShardCommitState> fileToBlobFile = new ConcurrentHashMap<>();
    private final Supplier<String> nodeEphemeralIdSupplier;
    private final ThreadContext threadContext;

    public StatelessCommitService(Supplier<String> nodeEphemeralIdSupplier, ThreadContext threadContext) {
        this.nodeEphemeralIdSupplier = nodeEphemeralIdSupplier;
        this.threadContext = threadContext;
    }

    public void markFileUploaded(ShardId shardId, String name, BlobLocation objectStoreLocation) {
        ShardCommitState commitState = getSafe(fileToBlobFile, shardId);
        synchronized (commitState) {
            commitState.fileMap.get(name).setBlobLocation(objectStoreLocation);
        }
    }

    public List<String> resolveMissingFiles(ShardId shardId, Collection<String> commitFiles) {
        ShardCommitState shardCommitState = getSafe(fileToBlobFile, shardId);
        synchronized (shardCommitState) {
            return commitFiles.stream()
                .filter(s -> s.startsWith(IndexFileNames.SEGMENTS) == false)
                .filter(f -> shardCommitState.fileMap.get(f).isUploaded() == false)
                .toList();
        }
    }

    public StatelessCompoundCommit.Writer returnPendingCompoundCommit(
        ShardId shardId,
        long generation,
        long primaryTerm,
        Map<String, Long> commitFiles
    ) {
        ShardCommitState shardCommitState = getSafe(fileToBlobFile, shardId);
        StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
            shardId,
            generation,
            primaryTerm,
            nodeEphemeralIdSupplier.get()
        );
        synchronized (shardCommitState) {
            for (Map.Entry<String, Long> commitFile : commitFiles.entrySet()) {
                String fileName = commitFile.getKey();
                if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
                    BlobFile blobFile = shardCommitState.fileMap.get(fileName);
                    assert blobFile.isUploaded();
                    writer.addReferencedBlobFile(fileName, blobFile.location());
                } else {
                    writer.addInternalFile(fileName, commitFile.getValue());
                }
            }
        }
        return writer;
    }

    public void markNewCommit(ShardId shardId, Collection<String> commitFiles, Set<String> additionalFiles) {
        ShardCommitState commitState = getSafe(fileToBlobFile, shardId);
        synchronized (commitState) {
            Map<String, BlobFile> fileMap = commitState.fileMap;
            for (String file : commitFiles) {
                if (additionalFiles.contains(file)) {
                    BlobFile existing = fileMap.put(file, new BlobFile());
                    assert existing == null;
                } else {
                    fileMap.get(file).incRef();
                }
            }
        }
    }

    public void markCommitUploaded(ShardId shardId, StatelessCompoundCommit commit) {
        ShardCommitState commitState = fileToBlobFile.get(shardId);
        // It is fine to ignore if that shard has been closed
        if (commitState != null) {
            commitState.markUploadedGeneration(commit.generation());
        }
    }

    public void markCommitDeleted(ShardId shardId, Collection<String> commitFiles) {
        ShardCommitState commitState = getSafe(fileToBlobFile, shardId);
        synchronized (commitState) {
            Map<String, BlobFile> fileMap = commitState.fileMap;
            for (String file : commitFiles) {
                boolean shouldRemove = fileMap.get(file).decRef();
                if (shouldRemove) {
                    fileMap.remove(file);
                }
            }
        }
    }

    public void register(ShardId shardId) {
        ShardCommitState existing = fileToBlobFile.put(shardId, new ShardCommitState(shardId));
        assert existing == null : shardId + " already registered";
    }

    public void unregister(ShardId shardId) {
        ShardCommitState removed = fileToBlobFile.remove(shardId);
        assert removed != null : shardId + " not registered";
        removed.close();
    }

    public void addOrNotify(ShardId shardId, long generation, ActionListener<Void> listener) {
        requireNonNull(listener, "listener cannot be null");
        ShardCommitState commitState = getSafe(fileToBlobFile, shardId);
        commitState.addOrNotify(generation, listener);
    }

    // Visible for testing
    Map<String, BlobFile> getFileToBlobFile(ShardId shardId) {
        return fileToBlobFile.get(shardId).fileMap;
    }

    private static ShardCommitState getSafe(ConcurrentHashMap<ShardId, ShardCommitState> map, ShardId shardId) {
        final ShardCommitState commitState = map.get(shardId);
        if (commitState == null) {
            throw new AlreadyClosedException("shard [" + shardId + "] has already been closed");
        }
        return commitState;
    }

    private class ShardCommitState {

        private final ShardId shardId;

        private final Map<String, BlobFile> fileMap = new HashMap<>();
        private List<Tuple<Long, ActionListener<Void>>> generationListeners = null;
        private long generationUploaded = -1;
        private boolean isClosed;

        private ShardCommitState(ShardId shardId) {
            this.shardId = shardId;
        }

        private void markUploadedGeneration(long newGeneration) {
            List<ActionListener<Void>> listenersToFire = null;
            List<Tuple<Long, ActionListener<Void>>> listenersToReregister = null;
            synchronized (this) {
                generationUploaded = Math.max(generationUploaded, newGeneration);

                // No listeners to check or generation did not increase so just bail early
                if (generationListeners == null || generationUploaded != newGeneration) {
                    return;
                }

                for (Tuple<Long, ActionListener<Void>> tuple : generationListeners) {
                    Long generation = tuple.v1();
                    if (generationUploaded >= generation) {
                        if (listenersToFire == null) {
                            listenersToFire = new ArrayList<>();
                        }
                        listenersToFire.add(tuple.v2());
                    } else {
                        if (listenersToReregister == null) {
                            listenersToReregister = new ArrayList<>();
                        }
                        listenersToReregister.add(tuple);
                    }
                }
                generationListeners = listenersToReregister;
            }

            if (listenersToFire != null) {
                ActionListener.onResponse(listenersToFire, null);
            }
        }

        private void addOrNotify(long generation, ActionListener<Void> listener) {

            boolean completeListenerSuccess = false;
            boolean completeListenerClosed = false;
            synchronized (this) {
                if (isClosed) {
                    completeListenerClosed = true;
                } else if (generationUploaded >= generation) {
                    // Location already visible, just call the listener
                    completeListenerSuccess = true;
                } else {
                    List<Tuple<Long, ActionListener<Void>>> listeners = generationListeners;
                    ActionListener<Void> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(
                        listener,
                        threadContext
                    );
                    if (listeners == null) {
                        listeners = new ArrayList<>();
                    }
                    listeners.add(new Tuple<>(generation, contextPreservingListener));
                    generationListeners = listeners;
                }
            }

            if (completeListenerClosed) {
                listener.onFailure(new AlreadyClosedException("shard [" + shardId + "] has already been closed"));
            } else if (completeListenerSuccess) {
                listener.onResponse(null);
            }
        }

        private void close() {
            List<Tuple<Long, ActionListener<Void>>> listenersToFail;
            synchronized (this) {
                isClosed = true;
                listenersToFail = generationListeners;
                generationListeners = null;
            }

            if (listenersToFail != null) {
                ActionListener.onFailure(
                    listenersToFail.stream().map(Tuple::v2).collect(Collectors.toList()),
                    new AlreadyClosedException("shard closed")
                );
            }
        }
    }

    private static class BlobFile extends AbstractRefCounted {

        private Optional<BlobLocation> blobLocation = Optional.empty();

        private void setBlobLocation(BlobLocation uploadedLocation) {
            // TODO: If a file is uploaded twice do we need to keep track of all locations? If it is not part of a compound file we might
            // need to in order to prune files from the object store. Currently we always upload the file to the same location, so there
            // is only one copy. But future optimization could potentially lead to multiple locations.
            if (this.blobLocation.isEmpty()) {
                this.blobLocation = Optional.of(uploadedLocation);
            }
        }

        private boolean isUploaded() {
            return blobLocation.isPresent();
        }

        private BlobLocation location() {
            return blobLocation.get();
        }

        @Override
        protected void closeInternal() {}
    }
}
