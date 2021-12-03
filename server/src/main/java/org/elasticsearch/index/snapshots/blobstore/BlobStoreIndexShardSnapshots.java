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
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * Contains information about all snapshots for the given shard in repository
 * <p>
 * This class is used to find files that were already snapshotted and clear out files that no longer referenced by any
 * snapshots.
 */
public class BlobStoreIndexShardSnapshots implements Iterable<SnapshotFiles>, ToXContentFragment {

    public static final BlobStoreIndexShardSnapshots EMPTY = new BlobStoreIndexShardSnapshots(Collections.emptyList());

    private final List<SnapshotFiles> shardSnapshots;
    private final Map<String, FileInfo> files;
    private final Map<String, List<FileInfo>> physicalFiles;

    public BlobStoreIndexShardSnapshots(List<SnapshotFiles> shardSnapshots) {
        this.shardSnapshots = List.copyOf(shardSnapshots);
        // Map between blob names and file info
        Map<String, FileInfo> newFiles = new HashMap<>();
        // Map between original physical names and file info
        Map<String, List<FileInfo>> physicalFiles = new HashMap<>();
        for (SnapshotFiles snapshot : shardSnapshots) {
            // First we build map between filenames in the repo and their original file info
            // this map will be used in the next loop
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                FileInfo oldFile = newFiles.put(fileInfo.name(), fileInfo);
                assert oldFile == null || oldFile.isSame(fileInfo);
            }
            // We are doing it in two loops here so we keep only one copy of the fileInfo per blob
            // the first loop de-duplicates fileInfo objects that were loaded from different snapshots but refer to
            // the same blob
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                physicalFiles.computeIfAbsent(fileInfo.physicalName(), k -> new ArrayList<>()).add(newFiles.get(fileInfo.name()));
            }
        }
        Map<String, List<FileInfo>> mapBuilder = new HashMap<>();
        for (Map.Entry<String, List<FileInfo>> entry : physicalFiles.entrySet()) {
            mapBuilder.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        this.physicalFiles = unmodifiableMap(mapBuilder);
        this.files = unmodifiableMap(newFiles);
    }

    private BlobStoreIndexShardSnapshots(Map<String, FileInfo> files, List<SnapshotFiles> shardSnapshots) {
        this.shardSnapshots = shardSnapshots;
        this.files = files;
        Map<String, List<FileInfo>> physicalFiles = new HashMap<>();
        for (SnapshotFiles snapshot : shardSnapshots) {
            for (FileInfo fileInfo : snapshot.indexFiles()) {
                physicalFiles.computeIfAbsent(fileInfo.physicalName(), k -> new ArrayList<>()).add(files.get(fileInfo.name()));
            }
        }
        Map<String, List<FileInfo>> mapBuilder = new HashMap<>();
        for (Map.Entry<String, List<FileInfo>> entry : physicalFiles.entrySet()) {
            mapBuilder.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
        this.physicalFiles = unmodifiableMap(mapBuilder);
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
        return new BlobStoreIndexShardSnapshots(CollectionUtils.appendToCopy(shardSnapshots, sourceFiles.withSnapshotName(target)));
    }

    /**
     * Returns list of snapshots
     *
     * @return list of snapshots
     */
    public List<SnapshotFiles> snapshots() {
        return this.shardSnapshots;
    }

    /**
     * Finds reference to a snapshotted file by its original name
     *
     * @param physicalName original name
     * @return a list of file infos that match specified physical file or null if the file is not present in any of snapshots
     */
    public List<FileInfo> findPhysicalIndexFiles(String physicalName) {
        return physicalFiles.get(physicalName);
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
    }

    static final class ParseFields {
        static final ParseField FILES = new ParseField("files");
        static final ParseField SHARD_STATE_ID = new ParseField("shard_state_id");
        static final ParseField SNAPSHOTS = new ParseField("snapshots");
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
                builder.field(ParseFields.SHARD_STATE_ID.getPreferredName(), snapshot.shardStateIdentifier());
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
        Map<String, List<String>> snapshotsMap = new HashMap<>();
        Map<String, String> historyUUIDs = new HashMap<>();
        Map<String, FileInfo> files = new HashMap<>();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String currentFieldName = parser.currentName();
            token = parser.nextToken();
            if (token == XContentParser.Token.START_ARRAY) {
                if (ParseFields.FILES.match(currentFieldName, parser.getDeprecationHandler()) == false) {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    FileInfo fileInfo = FileInfo.fromXContent(parser);
                    files.put(fileInfo.name(), fileInfo);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (ParseFields.SNAPSHOTS.match(currentFieldName, parser.getDeprecationHandler()) == false) {
                    XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                    String snapshot = parser.currentName();
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                            if (ParseFields.FILES.match(currentFieldName, parser.getDeprecationHandler())
                                && parser.nextToken() == XContentParser.Token.START_ARRAY) {
                                snapshotsMap.put(snapshot, XContentParserUtils.parseList(parser, XContentParser::text));
                            } else if (ParseFields.SHARD_STATE_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.nextToken();
                                historyUUIDs.put(snapshot, parser.text());
                            }
                        }
                    }
                }
            } else {
                XContentParserUtils.throwUnknownToken(token, parser.getTokenLocation());
            }
        }

        List<SnapshotFiles> snapshots = new ArrayList<>(snapshotsMap.size());
        for (Map.Entry<String, List<String>> entry : snapshotsMap.entrySet()) {
            List<FileInfo> fileInfosBuilder = new ArrayList<>();
            for (String file : entry.getValue()) {
                FileInfo fileInfo = files.get(file);
                assert fileInfo != null;
                fileInfosBuilder.add(fileInfo);
            }
            snapshots.add(
                new SnapshotFiles(entry.getKey(), Collections.unmodifiableList(fileInfosBuilder), historyUUIDs.get(entry.getKey()))
            );
        }
        return new BlobStoreIndexShardSnapshots(files, Collections.unmodifiableList(snapshots));
    }

}
