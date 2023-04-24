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
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class StatelessCommitService {

    // We don't do null checks when reading from this sub-map because we hold a commit reference while files are being uploaded. This will
    // prevent commit deletion in the interim.
    private final ConcurrentHashMap<ShardId, Map<String, BlobFile>> fileToBlobFile = new ConcurrentHashMap<>();
    private final Supplier<String> nodeEphemeralIdSupplier;

    public StatelessCommitService(Supplier<String> nodeEphemeralIdSupplier) {
        this.nodeEphemeralIdSupplier = nodeEphemeralIdSupplier;
    }

    public void markFileUploaded(ShardId shardId, String name, BlobLocation objectStoreLocation) {
        Map<String, BlobFile> fileMap = getSafe(fileToBlobFile, shardId);
        synchronized (fileMap) {
            fileMap.get(name).setBlobLocation(objectStoreLocation);
        }
    }

    public List<String> resolveMissingFiles(ShardId shardId, Collection<String> commitFiles) {
        Map<String, BlobFile> fileMap = getSafe(fileToBlobFile, shardId);
        synchronized (fileMap) {
            return commitFiles.stream()
                .filter(s -> s.startsWith(IndexFileNames.SEGMENTS) == false)
                .filter(f -> fileMap.get(f).isUploaded() == false)
                .toList();
        }
    }

    public StatelessCompoundCommit.Writer returnPendingCompoundCommit(
        ShardId shardId,
        long generation,
        long primaryTerm,
        Collection<StoreFileMetadata> commitFiles
    ) {
        Map<String, BlobFile> fileMap = getSafe(fileToBlobFile, shardId);
        StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
            shardId,
            generation,
            primaryTerm,
            nodeEphemeralIdSupplier.get()
        );
        synchronized (fileMap) {
            for (StoreFileMetadata commitFile : commitFiles) {
                String fileName = commitFile.name();
                if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
                    BlobFile blobFile = fileMap.get(fileName);
                    assert blobFile.isUploaded();
                    writer.addReferencedBlobFile(fileName, blobFile.location());
                } else {
                    writer.addInternalFile(commitFile);
                }
            }
        }
        return writer;
    }

    public void markNewCommit(ShardId shardId, Collection<String> commitFiles, Set<String> additionalFiles) {
        Map<String, BlobFile> fileMap = getSafe(fileToBlobFile, shardId);
        synchronized (fileMap) {
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

    public void markCommitDeleted(ShardId shardId, Collection<String> commitFiles) {
        Map<String, BlobFile> fileMap = getSafe(fileToBlobFile, shardId);
        synchronized (fileMap) {
            for (String file : commitFiles) {
                boolean shouldRemove = fileMap.get(file).decRef();
                if (shouldRemove) {
                    fileMap.remove(file);
                }
            }
        }
    }

    public void register(ShardId shardId) {
        Map<String, BlobFile> existing = fileToBlobFile.put(shardId, new HashMap<>());
        assert existing == null : shardId + " already registered";
    }

    public void unregister(ShardId shardId) {
        Map<String, BlobFile> removed = fileToBlobFile.remove(shardId);
        assert removed != null : shardId + " not registered";
    }

    // Visible for testing
    Map<String, BlobFile> getFileToBlobFile(ShardId shardId) {
        return fileToBlobFile.get(shardId);
    }

    private static Map<String, BlobFile> getSafe(ConcurrentHashMap<ShardId, Map<String, BlobFile>> map, ShardId shardId) {
        final Map<String, BlobFile> fileMap = map.get(shardId);
        if (fileMap == null) {
            throw new AlreadyClosedException("shard [" + shardId + "] has already been closed");
        }
        return fileMap;
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
