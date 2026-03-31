/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.GenericFileList;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Partition-grouped file listing for Hive-partitioned data. Stores column value
 * dictionaries and groups files by partition, reducing memory to ~32 bytes/file
 * vs ~52 bytes for DictionaryFileList. Falls back to DictionaryFileList when the
 * data is not Hive-partitioned.
 */
public final class HiveFileList implements FileList {

    private final String basePath;
    private final String[] partitionColumnNames;
    private final String[][] columnValueDicts;
    private final short[][] groupValueIndices;
    private final int[] groupFileStarts;
    private final long[] sizes;
    @Nullable
    private final long[] mtimesMillis;
    @Nullable
    private final long[] groupMtimes;
    private final String[] leafNames;
    @Nullable
    private final String sharedExtension;
    @Nullable
    private final String originalPattern;
    @Nullable
    private final PartitionMetadata partitionMetadata;
    private final int fileCount;

    HiveFileList(
        String basePath,
        String[] partitionColumnNames,
        String[][] columnValueDicts,
        short[][] groupValueIndices,
        int[] groupFileStarts,
        long[] sizes,
        @Nullable long[] mtimesMillis,
        @Nullable long[] groupMtimes,
        String[] leafNames,
        @Nullable String sharedExtension,
        @Nullable String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        int fileCount
    ) {
        this.basePath = basePath;
        this.partitionColumnNames = partitionColumnNames;
        this.columnValueDicts = columnValueDicts;
        this.groupValueIndices = groupValueIndices;
        this.groupFileStarts = groupFileStarts;
        this.sizes = sizes;
        this.mtimesMillis = mtimesMillis;
        this.groupMtimes = groupMtimes;
        this.leafNames = leafNames;
        this.sharedExtension = sharedExtension;
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        this.fileCount = fileCount;
    }

    /**
     * Builds a compact FileList from a GenericFileList. Returns HiveFileList if the data
     * has partition metadata, otherwise delegates to {@link DictionaryFileList#from}.
     */
    public static FileList from(String basePath, GenericFileList raw) {
        if (raw == null || raw.isResolved() == false || raw.fileCount() == 0) {
            return raw;
        }
        PartitionMetadata pm = raw.partitionMetadata();
        if (pm == null || pm.isEmpty()) {
            return DictionaryFileList.from(basePath, raw);
        }
        return buildHive(basePath, raw, pm);
    }

    private static FileList buildHive(String basePath, GenericFileList raw, PartitionMetadata pm) {
        List<StorageEntry> files = raw.files();
        int count = files.size();
        String[] colNames = pm.partitionColumns().keySet().toArray(new String[0]);
        int numCols = colNames.length;

        String normalizedBase = basePath;
        if (normalizedBase.isEmpty() == false && normalizedBase.endsWith("/") == false) {
            normalizedBase = normalizedBase + "/";
        }

        @SuppressWarnings("unchecked")
        Map<String, Short>[] colValMaps = new Map[numCols];
        @SuppressWarnings("unchecked")
        List<String>[] colValLists = new List[numCols];
        for (int c = 0; c < numCols; c++) {
            colValMaps[c] = new HashMap<>();
            colValLists[c] = new ArrayList<>();
        }

        Map<String, List<Integer>> groupMap = new LinkedHashMap<>();

        for (int f = 0; f < count; f++) {
            StoragePath sp = files.get(f).path();
            Map<String, Object> partVals = pm.filePartitionValues().get(sp);
            StringBuilder keyBuilder = new StringBuilder();
            for (int c = 0; c < numCols; c++) {
                String val = partVals != null ? String.valueOf(partVals.getOrDefault(colNames[c], "")) : "";
                Short idx = colValMaps[c].get(val);
                if (idx == null) {
                    if (colValLists[c].size() >= 65535) {
                        return DictionaryFileList.from(basePath, raw);
                    }
                    idx = (short) colValLists[c].size();
                    colValMaps[c].put(val, idx);
                    colValLists[c].add(val);
                }
                if (c > 0) {
                    keyBuilder.append('\0');
                }
                keyBuilder.append((int) idx);
            }
            String gk = keyBuilder.toString();
            groupMap.computeIfAbsent(gk, k -> new ArrayList<>()).add(f);
        }

        int numGroups = groupMap.size();
        short[][] groupValIndices = new short[numGroups][numCols];
        int[] groupFileStarts = new int[numGroups + 1];

        long[] orderedSizes = new long[count];
        String[] orderedLeafNames = new String[count];
        long[] orderedMtimes = new long[count];

        String sharedExt = null;
        boolean extChecked = false;

        int filePos = 0;
        int groupIdx = 0;
        for (Map.Entry<String, List<Integer>> gEntry : groupMap.entrySet()) {
            List<Integer> fileIndices = gEntry.getValue();
            groupFileStarts[groupIdx] = filePos;

            int firstFile = fileIndices.get(0);
            StoragePath firstPath = files.get(firstFile).path();
            Map<String, Object> firstPartVals = pm.filePartitionValues().get(firstPath);
            for (int c = 0; c < numCols; c++) {
                String val = firstPartVals != null ? String.valueOf(firstPartVals.getOrDefault(colNames[c], "")) : "";
                groupValIndices[groupIdx][c] = colValMaps[c].get(val);
            }

            for (int fi : fileIndices) {
                StorageEntry entry = files.get(fi);
                orderedSizes[filePos] = entry.length();
                orderedMtimes[filePos] = entry.lastModified().toEpochMilli();

                String fullPath = entry.path().toString();
                String relative = fullPath;
                if (normalizedBase.isEmpty() == false && fullPath.startsWith(normalizedBase)) {
                    relative = fullPath.substring(normalizedBase.length());
                }
                String leaf = relative;
                int lastSlash = relative.lastIndexOf('/');
                if (lastSlash >= 0) {
                    leaf = relative.substring(lastSlash + 1);
                }

                String ext = DictionaryFileList.extractExtension(leaf);
                if (extChecked == false) {
                    sharedExt = ext;
                    extChecked = true;
                } else if (sharedExt != null && (ext == null || sharedExt.equals(ext) == false)) {
                    sharedExt = null;
                }

                orderedLeafNames[filePos] = leaf;
                filePos++;
            }
            groupIdx++;
        }
        groupFileStarts[numGroups] = filePos;

        if (sharedExt != null) {
            for (int i = 0; i < count; i++) {
                if (orderedLeafNames[i].endsWith(sharedExt)) {
                    orderedLeafNames[i] = orderedLeafNames[i].substring(0, orderedLeafNames[i].length() - sharedExt.length());
                }
            }
        }

        boolean uniformMtimes = true;
        long[] gMtimes = new long[numGroups];
        outer: for (int g = 0; g < numGroups; g++) {
            int start = groupFileStarts[g];
            int end = groupFileStarts[g + 1];
            long groupMtime = orderedMtimes[start];
            gMtimes[g] = groupMtime;
            for (int i = start + 1; i < end; i++) {
                if (orderedMtimes[i] != groupMtime) {
                    uniformMtimes = false;
                    break outer;
                }
            }
        }

        String[][] colValDicts = new String[numCols][];
        for (int c = 0; c < numCols; c++) {
            colValDicts[c] = colValLists[c].toArray(new String[0]);
        }

        return new HiveFileList(
            normalizedBase,
            colNames,
            colValDicts,
            groupValIndices,
            groupFileStarts,
            orderedSizes,
            uniformMtimes ? null : orderedMtimes,
            uniformMtimes ? gMtimes : null,
            orderedLeafNames,
            sharedExt,
            raw.originalPattern(),
            raw.partitionMetadata(),
            count
        );
    }

    private int findGroup(int fileIndex) {
        int lo = 0;
        int hi = groupFileStarts.length - 2;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (groupFileStarts[mid] <= fileIndex) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo;
    }

    @Override
    public int fileCount() {
        return fileCount;
    }

    @Override
    public StoragePath path(int i) {
        int group = findGroup(i);
        StringBuilder sb = new StringBuilder(basePath);
        for (int c = 0; c < partitionColumnNames.length; c++) {
            sb.append(partitionColumnNames[c]).append('=');
            sb.append(columnValueDicts[c][Short.toUnsignedInt(groupValueIndices[group][c])]);
            sb.append('/');
        }
        sb.append(leafNames[i]);
        if (sharedExtension != null) {
            sb.append(sharedExtension);
        }
        return StoragePath.of(sb.toString());
    }

    @Override
    public long size(int i) {
        return sizes[i];
    }

    @Override
    public long lastModifiedMillis(int i) {
        if (groupMtimes != null) {
            return groupMtimes[findGroup(i)];
        }
        return mtimesMillis[i];
    }

    @Override
    @Nullable
    public String originalPattern() {
        return originalPattern;
    }

    @Override
    @Nullable
    public PartitionMetadata partitionMetadata() {
        return partitionMetadata;
    }

    @Override
    public boolean isResolved() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return fileCount == 0;
    }

    @Override
    public long estimatedBytes() {
        // object header + reference fields
        long bytes = 64;
        // basePath String
        bytes += basePath.length() * (long) Character.BYTES;
        for (String[] dict : columnValueDicts) {
            for (String v : dict) {
                // per-String: ~40B object overhead + char data
                bytes += 40 + v.length() * (long) Character.BYTES;
            }
        }
        // partition value indices: groups × columns × short
        bytes += (long) groupValueIndices.length * partitionColumnNames.length * Short.BYTES;
        bytes += (long) groupFileStarts.length * Integer.BYTES;
        bytes += sizes.length * (long) Long.BYTES;
        if (mtimesMillis != null) {
            bytes += mtimesMillis.length * (long) Long.BYTES;
        }
        if (groupMtimes != null) {
            bytes += groupMtimes.length * (long) Long.BYTES;
        }
        for (String leaf : leafNames) {
            bytes += 40 + leaf.length() * (long) Character.BYTES;
        }
        if (sharedExtension != null) {
            bytes += 40 + sharedExtension.length() * (long) Character.BYTES;
        }
        return bytes;
    }
}
