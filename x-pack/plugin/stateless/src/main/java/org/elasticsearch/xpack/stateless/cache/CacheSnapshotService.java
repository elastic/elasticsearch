/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Coordinates the creation of a point-in-time snapshot of the shared blob cache. The snapshot
 * consists of two parts: a JSON metadata file that records which cache slots are occupied and
 * what byte ranges have been downloaded, and a cloud-provider volume snapshot of the underlying
 * cache file.
 *
 * <p>The process is:
 * <ol>
 *   <li>Freeze the cache (drains in-flight gap fills and acquires the write stamp).</li>
 *   <li>Collect the occupied entry list via {@link SharedBlobCacheService#getOccupiedEntries()}.</li>
 *   <li>Issue {@code SYNC_FILE_RANGE_WRITE} for every slot, then {@code SYNC_FILE_RANGE_WAIT_AFTER}
 *       to ensure the kernel page-cache is written to disk before the volume snapshot.</li>
 *   <li>Write the JSON metadata file atomically.</li>
 *   <li>Trigger the cloud provider volume snapshot.</li>
 *   <li>Unfreeze the cache.</li>
 * </ol>
 */
public class CacheSnapshotService {

    private static final Logger logger = LogManager.getLogger(CacheSnapshotService.class);

    static final int SNAPSHOT_FORMAT_VERSION = 1;

    /**
     * Returned by {@link #readSnapshotFile} when no valid snapshot file is present or when
     * the file cannot be parsed. {@link SnapshotMetadata#sourceNodeId()} will be {@code null}.
     */
    public static final SnapshotMetadata EMPTY = new SnapshotMetadata(null, List.of());

    /** Metadata decoded from a previously written snapshot file. */
    public record SnapshotMetadata(String sourceNodeId, List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> entries) {}

    private final Path snapshotFilePath;
    private final CloudVolumeSnapshotProvider cloudProvider;

    /**
     * @param snapshotFilePath the path at which the JSON metadata file is written
     * @param cloudProvider    the cloud-volume snapshot provider
     */
    public CacheSnapshotService(Path snapshotFilePath, CloudVolumeSnapshotProvider cloudProvider) {
        this.snapshotFilePath = snapshotFilePath;
        this.cloudProvider = cloudProvider;
    }

    /**
     * Takes a snapshot of the given cache, writing the metadata file and triggering a cloud volume
     * snapshot. Returns the provider-assigned snapshot ID.
     *
     * @param cache  the stateless shared blob cache to snapshot
     * @param nodeId the ID of the local node, embedded in the metadata for the replacement node
     * @return the cloud provider snapshot ID
     * @throws IOException if any I/O step fails
     */
    public String snapshot(StatelessSharedBlobCacheService cache, String nodeId) throws IOException {
        long stamp = cache.freeze();
        try {
            List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> entries = cache.getOccupiedEntries();

            // Phase 1: initiate async write-back for all occupied slots.
            for (SharedBlobCacheService.CacheIndexEntry<FileCacheKey> entry : entries) {
                cache.syncSlotRange(entry.physicalSlot(), SharedBytes.SYNC_FILE_RANGE_WRITE);
            }

            // Phase 2: wait for write-back to complete for all occupied slots.
            for (SharedBlobCacheService.CacheIndexEntry<FileCacheKey> entry : entries) {
                cache.syncSlotRange(entry.physicalSlot(), SharedBytes.SYNC_FILE_RANGE_WAIT_AFTER);
            }

            writeSnapshotFile(nodeId, entries, cache.getNumRegions(), cache.getRegionSize());

            String snapshotId;
            try {
                snapshotId = cloudProvider.createSnapshot();
            } catch (Exception e) {
                // The metadata file was written but the cloud volume snapshot was never committed.
                // Delete it so that a subsequent node startup does not attempt to restore from a
                // snapshot that does not exist in the cloud.
                try {
                    Files.deleteIfExists(snapshotFilePath);
                } catch (IOException deleteEx) {
                    e.addSuppressed(deleteEx);
                }
                throw e;
            }
            logger.info("created cache snapshot [{}] for node [{}] with [{}] occupied regions", snapshotId, nodeId, entries.size());
            return snapshotId;
        } finally {
            cache.unfreeze(stamp);
        }
    }

    /**
     * Writes the snapshot metadata file atomically. Writes to a {@code .tmp} file, forces it to
     * disk via {@code FileChannel.force}, then renames it into place.
     */
    void writeSnapshotFile(
        String nodeId,
        List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> entries,
        int numRegions,
        int regionSize
    ) throws IOException {
        Path tmp = snapshotFilePath.resolveSibling(snapshotFilePath.getFileName() + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp.toFile())) {
            // XContentBuilder closes the underlying stream on exit, so we must force the channel
            // to disk before the builder's try-with-resources block closes it.
            try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), fos)) {
                builder.startObject();
                builder.field("version", SNAPSHOT_FORMAT_VERSION);
                builder.field("num_regions", numRegions);
                builder.field("region_size", regionSize);
                builder.field("node_id", nodeId);
                builder.startArray("entries");
                for (SharedBlobCacheService.CacheIndexEntry<FileCacheKey> entry : entries) {
                    builder.startObject();
                    builder.field("slot", entry.physicalSlot());
                    FileCacheKey key = entry.key();
                    builder.startObject("key");
                    builder.field("index", key.shardId().getIndexName());
                    builder.field("shard", key.shardId().id());
                    builder.field("primary_term", key.primaryTerm());
                    builder.field("file_name", key.fileName());
                    builder.endObject();
                    builder.field("region", entry.region());
                    builder.field("effective_region_size", entry.effectiveRegionSize());
                    builder.startArray("ranges");
                    for (ByteRange range : entry.completedRanges()) {
                        builder.startObject();
                        builder.field("start", range.start());
                        builder.field("end", range.end());
                        builder.endObject();
                    }
                    builder.endArray();
                    builder.endObject();
                }
                builder.endArray();
                builder.endObject();
                builder.flush();
                fos.getChannel().force(false);
            }
        }
        Files.move(tmp, snapshotFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Reads a previously written snapshot metadata file. Returns {@link #EMPTY} on any error
     * (missing file, parse failure, version mismatch) so that the caller can always proceed
     * without a snapshot.
     *
     * @param snapshotFile  the path of the snapshot metadata file
     * @param numRegions    the current cache's region count, used to validate slot indices
     * @param regionSize    the current cache's region size in bytes
     * @return the decoded metadata, or {@link #EMPTY}
     */
    public static SnapshotMetadata readSnapshotFile(Path snapshotFile, int numRegions, int regionSize) {
        if (Files.exists(snapshotFile) == false) {
            return EMPTY;
        }
        try (var stream = Files.newInputStream(snapshotFile)) {
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, stream)) {
                return parseSnapshotMetadata(parser, numRegions, regionSize);
            }
        } catch (Exception e) {
            logger.warn("failed to read cache snapshot file [{}], ignoring", snapshotFile, e);
            return EMPTY;
        }
    }

    private static SnapshotMetadata parseSnapshotMetadata(XContentParser parser, int numRegions, int regionSize) throws IOException {
        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            return EMPTY;
        }
        int version = -1;
        int snapshotNumRegions = -1;
        int snapshotRegionSize = -1;
        String nodeId = null;
        List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> entries = new ArrayList<>();

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case "version" -> version = parser.intValue();
                case "num_regions" -> snapshotNumRegions = parser.intValue();
                case "region_size" -> snapshotRegionSize = parser.intValue();
                case "node_id" -> nodeId = parser.text();
                case "entries" -> entries = parseEntries(parser, numRegions);
                default -> parser.skipChildren();
            }
        }

        if (version != SNAPSHOT_FORMAT_VERSION) {
            logger.warn("unsupported snapshot version [{}]; starting cold", version);
            return EMPTY;
        }
        if (snapshotNumRegions != numRegions) {
            logger.warn("snapshot num_regions mismatch [{}] != [{}]; starting cold", snapshotNumRegions, numRegions);
            return EMPTY;
        }
        if (snapshotRegionSize != regionSize) {
            logger.warn("snapshot region_size mismatch [{}] != [{}]; starting cold", snapshotRegionSize, regionSize);
            return EMPTY;
        }
        if (nodeId == null) {
            logger.warn("snapshot missing node_id; starting cold");
            return EMPTY;
        }
        return new SnapshotMetadata(nodeId, entries);
    }

    private static List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> parseEntries(XContentParser parser, int numRegions)
        throws IOException {
        List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> result = new ArrayList<>();
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            return result;
        }
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            SharedBlobCacheService.CacheIndexEntry<FileCacheKey> entry = parseEntry(parser, numRegions);
            if (entry != null) {
                result.add(entry);
            }
        }
        return result;
    }

    private static SharedBlobCacheService.CacheIndexEntry<FileCacheKey> parseEntry(XContentParser parser, int numRegions)
        throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            return null;
        }
        int slot = -1;
        FileCacheKey key = null;
        int region = -1;
        int effectiveRegionSize = -1;
        SortedSet<ByteRange> ranges = new TreeSet<>(Comparator.comparingLong(ByteRange::start));

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case "slot" -> slot = parser.intValue();
                case "key" -> key = parseFileCacheKey(parser);
                case "region" -> region = parser.intValue();
                case "effective_region_size" -> effectiveRegionSize = parser.intValue();
                case "ranges" -> ranges = parseRanges(parser);
                default -> parser.skipChildren();
            }
        }

        if (slot < 0 || slot >= numRegions || key == null || region < 0 || effectiveRegionSize < 0 || ranges.isEmpty()) {
            return null;
        }
        return new SharedBlobCacheService.CacheIndexEntry<>(slot, key, region, effectiveRegionSize, ranges);
    }

    private static FileCacheKey parseFileCacheKey(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            return null;
        }
        String indexName = null;
        int shardId = -1;
        long primaryTerm = -1;
        String fileName = null;

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            switch (fieldName) {
                case "index" -> indexName = parser.text();
                case "shard" -> shardId = parser.intValue();
                case "primary_term" -> primaryTerm = parser.longValue();
                case "file_name" -> fileName = parser.text();
                default -> parser.skipChildren();
            }
        }

        if (indexName == null || shardId < 0 || primaryTerm < 0 || fileName == null) {
            return null;
        }
        return new FileCacheKey(new ShardId(new Index(indexName, "_na_"), shardId), primaryTerm, fileName);
    }

    private static SortedSet<ByteRange> parseRanges(XContentParser parser) throws IOException {
        SortedSet<ByteRange> ranges = new TreeSet<>(Comparator.comparingLong(ByteRange::start));
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            return ranges;
        }
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                continue;
            }
            long start = -1;
            long end = -1;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String fieldName = parser.currentName();
                parser.nextToken();
                switch (fieldName) {
                    case "start" -> start = parser.longValue();
                    case "end" -> end = parser.longValue();
                    default -> parser.skipChildren();
                }
            }
            if (start >= 0 && end >= start) {
                ranges.add(ByteRange.of(start, end));
            }
        }
        return ranges;
    }

    /**
     * Returns the path of the snapshot metadata file that corresponds to the given cache file
     * path. The metadata file lives next to the cache file with a {@code .snapshot} suffix.
     *
     * @param cacheFilePath the path of the shared blob cache file
     * @return the path of the corresponding snapshot metadata file
     */
    public static Path snapshotFilePath(Path cacheFilePath) {
        String filename = cacheFilePath.getFileName().toString();
        return cacheFilePath.resolveSibling(filename + ".snapshot");
    }

    /**
     * Reads only the {@code node_id} field from a snapshot metadata file. Used during bootstrap
     * before cache dimensions are known. Returns empty on any error.
     */
    public static Optional<String> readSourceNodeId(Path snapshotFile) {
        if (Files.exists(snapshotFile) == false) {
            return Optional.empty();
        }
        try (var stream = Files.newInputStream(snapshotFile)) {
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, stream)) {
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    return Optional.empty();
                }
                String nodeId = null;
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    if ("node_id".equals(fieldName)) {
                        nodeId = parser.text();
                    } else {
                        parser.skipChildren();
                    }
                }
                if (nodeId == null || nodeId.isBlank()) {
                    return Optional.empty();
                }
                return Optional.of(nodeId);
            }
        } catch (Exception e) {
            logger.warn("failed to read source node id from cache snapshot file [{}]", snapshotFile, e);
            return Optional.empty();
        }
    }
}
