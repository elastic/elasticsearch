/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Contains information about all snapshots for the given shard in repository
 * <p>
 * This class is used to find shard files that were already snapshotted and clear out shard files that are no longer referenced by any
 * snapshots of the shard.
 */
public class BlobStoreIndexShardSnapshots implements Iterable<SnapshotFiles>, ToXContentFragment {

    public static final BlobStoreIndexShardSnapshots EMPTY = new BlobStoreIndexShardSnapshots(Map.of(), List.of());

    private final List<SnapshotFiles> shardSnapshots;
    private final Map<String, FileInfo> files;

    private BlobStoreIndexShardSnapshots(Map<String, FileInfo> files, List<SnapshotFiles> shardSnapshots) {
        this.shardSnapshots = List.copyOf(shardSnapshots);
        this.files = files;
    }

    /**
     * Creates a new list of the shard's snapshots ({@link BlobStoreIndexShardSnapshots}) retaining only the shard snapshots
     * specified by ID in {@code retainedSnapshots}. Typically used for snapshot deletions, which reduce the shard snapshots.
     */
    public BlobStoreIndexShardSnapshots withRetainedSnapshots(Set<SnapshotId> retainedSnapshots) {
        if (retainedSnapshots.isEmpty()) {
            return EMPTY;
        }
        final var survivingSnapshotNames = retainedSnapshots.stream().map(SnapshotId::getName).collect(Collectors.toSet());
        final ArrayList<SnapshotFiles> updatedSnapshots = new ArrayList<>(survivingSnapshotNames.size());
        Map<String, FileInfo> newFiles = new HashMap<>();
        for (SnapshotFiles snapshot : shardSnapshots) {
            if (survivingSnapshotNames.contains(snapshot.snapshot()) == false) {
                continue;
            }
            updatedSnapshots.add(snapshot);
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                FileInfo oldFile = newFiles.put(fileInfo.name(), fileInfo);
                assert oldFile == null || oldFile.isSame(fileInfo);
            }
        }
        return new BlobStoreIndexShardSnapshots(newFiles, updatedSnapshots);
    }

    /**
     * Creates a new list of the shard's snapshots ({@link BlobStoreIndexShardSnapshots}) adding a new shard snapshot
     * ({@link SnapshotFiles}).
     */
    public BlobStoreIndexShardSnapshots withAddedSnapshot(SnapshotFiles snapshotFiles) {
        Map<String, FileInfo> updatedFiles = null;
        for (FileInfo fileInfo : snapshotFiles.indexFiles()) {
            final FileInfo known = files.get(fileInfo.name());
            if (known == null) {
                if (updatedFiles == null) {
                    updatedFiles = new HashMap<>(files);
                }
                updatedFiles.put(fileInfo.name(), fileInfo);
            } else {
                assert fileInfo.isSame(known);
            }
        }
        return new BlobStoreIndexShardSnapshots(
            updatedFiles == null ? files : updatedFiles,
            CollectionUtils.appendToCopyNoNullElements(shardSnapshots, snapshotFiles)
        );
    }

    /**
     * Create a new instance that has a new snapshot by name {@code target} added which shares all files with the snapshot of name
     * {@code source}.
     *
     * @param source source snapshot name
     * @param target target snapshot name
     * @return new instance with added cloned snapshot
     */
    public BlobStoreIndexShardSnapshots withClone(String source, String target) {
        SnapshotFiles sourceFiles = null;
        for (SnapshotFiles shardSnapshot : shardSnapshots) {
            if (shardSnapshot.snapshot().equals(source)) {
                sourceFiles = shardSnapshot;
                break;
            }
        }
        if (sourceFiles == null) {
            throw new IllegalArgumentException("unknown source [" + source + "]");
        }
        return new BlobStoreIndexShardSnapshots(
            files,
            CollectionUtils.appendToCopyNoNullElements(shardSnapshots, sourceFiles.withSnapshotName(target))
        );
    }

    /**
     * Returns list of snapshots
     *
     * @return list of snapshots
     */
    public List<SnapshotFiles> snapshots() {
        return this.shardSnapshots;
    }

    // index of Lucene file name to collection of file info in the repository
    // lazy computed because building this is map is rather expensive and only needed for the snapshot create operation
    private Map<String, Collection<FileInfo>> physicalFiles;

    /**
     * Finds reference to a snapshotted file by its {@link StoreFileMetadata}
     *
     * @param storeFileMetadata store file metadata to find file info for
     * @return the file info that matches the specified physical file or null if the file is not present in any of snapshots
     */
    public FileInfo findPhysicalIndexFile(StoreFileMetadata storeFileMetadata) {
        var p = this.physicalFiles;
        if (p == null) {
            p = new HashMap<>();
            for (SnapshotFiles snapshot : shardSnapshots) {
                for (FileInfo fileInfo : snapshot.indexFiles()) {
                    // we use identity hash set since we lookup all instances from the same map and thus equality == instance equality
                    // and we don't want to add the same file to the map multiple times
                    p.computeIfAbsent(fileInfo.physicalName(), k -> Collections.newSetFromMap(new IdentityHashMap<>()))
                        .add(files.get(fileInfo.name()));
                }
            }
            physicalFiles = p;
        }
        final var found = p.get(storeFileMetadata.name());
        if (found == null) {
            return null;
        }
        for (FileInfo fileInfo : found) {
            if (fileInfo.isSame(storeFileMetadata)) {
                return fileInfo;
            }
        }
        return null;
    }

    /**
     * Finds reference to a snapshotted file by its snapshot name
     *
     * @param name file name
     * @return file info or null if file is not present in any of snapshots
     */
    public FileInfo findNameFile(String name) {
        return files.get(name);
    }

    @Override
    public Iterator<SnapshotFiles> iterator() {
        return shardSnapshots.iterator();
    }

    static final class Fields {
        static final String FILES = "files";
        static final String SNAPSHOTS = "snapshots";
        static final String SHARD_STATE_ID = "shard_state_id";
    }

    /**
     * Writes index file for the shard in the following format.
     * <pre>
     * <code>
     * {
     *     "files": [{
     *         "name": "__3",
     *         "physical_name": "_0.si",
     *         "length": 310,
     *         "checksum": "1tpsg3p",
     *         "written_by": "5.1.0",
     *         "meta_hash": "P9dsFxNMdWNlb......"
     *     }, {
     *         "name": "__2",
     *         "physical_name": "segments_2",
     *         "length": 150,
     *         "checksum": "11qjpz6",
     *         "written_by": "5.1.0",
     *         "meta_hash": "P9dsFwhzZWdtZ......."
     *     }, {
     *         "name": "__1",
     *         "physical_name": "_0.cfe",
     *         "length": 363,
     *         "checksum": "er9r9g",
     *         "written_by": "5.1.0"
     *     }, {
     *         "name": "__0",
     *         "physical_name": "_0.cfs",
     *         "length": 3354,
     *         "checksum": "491liz",
     *         "written_by": "5.1.0"
     *     }, {
     *         "name": "__4",
     *         "physical_name": "segments_3",
     *         "length": 150,
     *         "checksum": "134567",
     *         "written_by": "5.1.0",
     *         "meta_hash": "P9dsFwhzZWdtZ......."
     *     }],
     *     "snapshots": {
     *         "snapshot_1": {
     *             "files": ["__0", "__1", "__2", "__3"]
     *         },
     *         "snapshot_2": {
     *             "files": ["__0", "__1", "__2", "__4"]
     *         }
     *     }
     * }
     * }
     * </code>
     * </pre>
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // First we list all blobs with their file infos:
        builder.startArray(Fields.FILES);
        for (Map.Entry<String, FileInfo> entry : files.entrySet()) {
            FileInfo.toXContent(entry.getValue(), builder, params);
        }
        builder.endArray();
        // Then we list all snapshots with list of all blobs that are used by the snapshot
        builder.startObject(Fields.SNAPSHOTS);
        for (SnapshotFiles snapshot : shardSnapshots) {
            builder.startObject(snapshot.snapshot());
            builder.startArray(Fields.FILES);
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                builder.value(fileInfo.name());
            }
            builder.endArray();
            if (snapshot.shardStateIdentifier() != null) {
                builder.field(Fields.SHARD_STATE_ID, snapshot.shardStateIdentifier());
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static BlobStoreIndexShardSnapshots fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) { // New parser
            token = parser.nextToken();
        }
        // list of tuples of snapshot name and file ids in the snapshot
        List<Tuple<String, List<String>>> snapshotsAndFiles = new ArrayList<>();
        Map<String, String> historyUUIDs = new HashMap<>();
        Map<String, FileInfo> files = new HashMap<>();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        String currentFieldName;
        while ((currentFieldName = parser.nextFieldName()) != null) {
            token = parser.nextToken();
            switch (token) {
                case START_ARRAY -> {
                    if (Fields.FILES.equals(currentFieldName) == false) {
                        XContentParserUtils.throwUnknownField(currentFieldName, parser);
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        FileInfo fileInfo = FileInfo.fromXContent(parser);
                        files.put(fileInfo.name(), fileInfo);
                    }
                }
                case START_OBJECT -> {
                    if (Fields.SNAPSHOTS.equals(currentFieldName) == false) {
                        XContentParserUtils.throwUnknownField(currentFieldName, parser);
                    }
                    String snapshot;
                    while ((snapshot = parser.nextFieldName()) != null) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        while ((currentFieldName = parser.nextFieldName()) != null) {
                            token = parser.nextToken();
                            if (Fields.FILES.equals(currentFieldName)) {
                                if (token == XContentParser.Token.START_ARRAY) {
                                    snapshotsAndFiles.add(
                                        Tuple.tuple(snapshot, XContentParserUtils.parseList(parser, XContentParser::text))
                                    );
                                }
                            } else if (Fields.SHARD_STATE_ID.equals(currentFieldName)) {
                                historyUUIDs.put(snapshot, parser.text());
                            }
                        }
                    }
                }
                default -> XContentParserUtils.throwUnknownToken(token, parser);
            }
        }

        List<SnapshotFiles> snapshots = new ArrayList<>(snapshotsAndFiles.size());
        for (Tuple<String, List<String>> entry : snapshotsAndFiles) {
            List<FileInfo> fileInfosBuilder = new ArrayList<>();
            for (String file : entry.v2()) {
                FileInfo fileInfo = files.get(file);
                assert fileInfo != null;
                fileInfosBuilder.add(fileInfo);
            }
            snapshots.add(new SnapshotFiles(entry.v1(), Collections.unmodifiableList(fileInfosBuilder), historyUUIDs.get(entry.v1())));
        }
        return new BlobStoreIndexShardSnapshots(files, snapshots);
    }

}
