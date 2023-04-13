/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.index.IndexFileNames;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatelessCommitService {

    private final ConcurrentHashMap<ShardId, Map<String, BlobLocation>> fileToObjectStoreLocation = new ConcurrentHashMap<>();

    public void markFileUploaded(ShardId shardId, String name, BlobLocation objectStoreLocation) {
        Map<String, BlobLocation> fileMap = fileToObjectStoreLocation.computeIfAbsent(shardId, (k) -> new HashMap<>());
        synchronized (fileMap) {
            fileMap.putIfAbsent(name, objectStoreLocation);
        }
    }

    public List<String> resolveMissingFiles(ShardId shardId, Collection<String> commitFiles) {
        Map<String, BlobLocation> fileMap = fileToObjectStoreLocation.computeIfAbsent(shardId, (k) -> new HashMap<>());
        synchronized (fileMap) {
            return commitFiles.stream()
                .filter(s -> s.startsWith(IndexFileNames.SEGMENTS) == false)
                .filter(f -> fileMap.containsKey(f) == false)
                .toList();
        }
    }

    public StatelessCompoundCommit.Writer returnPendingCompoundCommit(
        ShardId shardId,
        long generation,
        long primaryTerm,
        Collection<StoreFileMetadata> commitFiles
    ) {
        Map<String, BlobLocation> fileMap = fileToObjectStoreLocation.computeIfAbsent(shardId, (k) -> new HashMap<>());
        StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(shardId, generation, primaryTerm);
        synchronized (fileMap) {
            for (StoreFileMetadata commitFile : commitFiles) {
                String fileName = commitFile.name();
                if (fileName.startsWith(IndexFileNames.SEGMENTS) == false) {
                    BlobLocation location = fileMap.get(fileName);
                    assert location != null;
                    writer.addReferencedBlobFile(fileName, location);
                } else {
                    writer.addInternalFile(commitFile);
                }
            }
        }
        return writer;
    }
}
