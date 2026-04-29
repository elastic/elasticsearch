/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

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
 * Compresses a {@link GenericFileList} into a compact representation.
 * Tries Hive-partitioned encoding first (if partition metadata exists),
 * then falls back to segment-dictionary encoding, and finally returns
 * the original list unchanged if neither encoding fits.
 */
final class FileListCompactor {

    private FileListCompactor() {}

    /**
     * Compacts a raw file list into the most efficient representation available.
     * Returns the original list when compaction is not applicable or overflows.
     * <p>
     * The hive-partitioning flag is not passed explicitly; instead, the presence
     * of {@link PartitionMetadata} on the raw list serves as the signal.
     * When {@code hivePartitioning=false} upstream, {@link GlobExpander} never
     * attaches partition metadata so the Hive branch here is skipped automatically
     * and we go straight to dictionary encoding.
     */
    static FileList compact(String basePath, GenericFileList raw) {
        if (raw == null || raw.isResolved() == false || raw.fileCount() == 0) {
            return raw;
        }
        String normalizedBase = normalizeBase(basePath);
        PartitionMetadata pm = raw.partitionMetadata();
        if (pm != null && pm.isEmpty() == false) {
            FileList hive = tryHive(normalizedBase, raw, pm);
            if (hive != null) {
                return hive;
            }
        }
        FileList dict = tryDictionary(normalizedBase, raw);
        if (dict != null) {
            return dict;
        }
        return raw;
    }

    private static String extractExtension(String leafSegment) {
        int dot = leafSegment.indexOf('.');
        if (dot >= 0 && (leafSegment.length() - dot) >= 4) {
            return leafSegment.substring(dot);
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Hive-partitioned encoding
    // ------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static FileList tryHive(String normalizedBase, GenericFileList raw, PartitionMetadata pm) {
        List<StorageEntry> files = raw.files();
        int count = files.size();
        String[] colNames = pm.partitionColumns().keySet().toArray(new String[0]);
        int numCols = colNames.length;

        Map<String, Short>[] colValMaps = new Map[numCols];
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
                        return null;
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

                String ext = extractExtension(leaf);
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

    // ------------------------------------------------------------------
    // Dictionary-encoded encoding
    // ------------------------------------------------------------------

    private static FileList tryDictionary(String normalizedBase, GenericFileList raw) {
        List<StorageEntry> files = raw.files();
        int count = files.size();
        long[] sizes = new long[count];
        long[] mtimes = new long[count];

        Map<String, Short> tokenMap = new HashMap<>();
        List<String> tokenList = new ArrayList<>();
        List<short[]> fileTokensList = new ArrayList<>(count);
        String sharedExt = null;
        boolean extensionChecked = false;

        for (int f = 0; f < count; f++) {
            StorageEntry entry = files.get(f);
            sizes[f] = entry.length();
            mtimes[f] = entry.lastModified().toEpochMilli();

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
            String ext = extractExtension(leaf);

            if (extensionChecked == false) {
                sharedExt = ext;
                extensionChecked = true;
            } else if (sharedExt != null && sharedExt.equals(ext) == false) {
                sharedExt = null;
            }

            String[] segments = relative.split("/");
            short[] tokenIndices = new short[segments.length];
            for (int s = 0; s < segments.length; s++) {
                Short idx = tokenMap.get(segments[s]);
                if (idx == null) {
                    if (tokenList.size() >= 65535) {
                        return null;
                    }
                    idx = (short) tokenList.size();
                    tokenMap.put(segments[s], idx);
                    tokenList.add(segments[s]);
                }
                tokenIndices[s] = idx;
            }
            fileTokensList.add(tokenIndices);
        }

        if (sharedExt != null) {
            tokenMap.clear();
            tokenList.clear();
            List<short[]> rebuiltTokens = new ArrayList<>(count);
            for (int f = 0; f < count; f++) {
                StorageEntry entry = files.get(f);
                String fullPath = entry.path().toString();
                String relative = fullPath;
                if (normalizedBase.isEmpty() == false && fullPath.startsWith(normalizedBase)) {
                    relative = fullPath.substring(normalizedBase.length());
                }

                String[] segments = relative.split("/");
                String lastSeg = segments[segments.length - 1];
                if (lastSeg.endsWith(sharedExt)) {
                    segments[segments.length - 1] = lastSeg.substring(0, lastSeg.length() - sharedExt.length());
                }

                short[] tokenIndices = new short[segments.length];
                for (int s = 0; s < segments.length; s++) {
                    Short idx = tokenMap.get(segments[s]);
                    if (idx == null) {
                        if (tokenList.size() >= 65535) {
                            return null;
                        }
                        idx = (short) tokenList.size();
                        tokenMap.put(segments[s], idx);
                        tokenList.add(segments[s]);
                    }
                    tokenIndices[s] = idx;
                }
                rebuiltTokens.add(tokenIndices);
            }
            fileTokensList = rebuiltTokens;
        }

        int totalTokens = 0;
        for (short[] t : fileTokensList) {
            totalTokens += t.length;
        }
        short[] flatTokens = new short[totalTokens];
        int[] starts = new int[count + 1];
        int pos = 0;
        for (int f = 0; f < count; f++) {
            starts[f] = pos;
            short[] t = fileTokensList.get(f);
            System.arraycopy(t, 0, flatTokens, pos, t.length);
            pos += t.length;
        }
        starts[count] = pos;

        String[] tokenArray = tokenList.toArray(new String[0]);

        return new DictionaryFileList(
            normalizedBase,
            tokenArray,
            flatTokens,
            starts,
            sizes,
            mtimes,
            sharedExt,
            raw.originalPattern(),
            raw.partitionMetadata(),
            count
        );
    }

    private static String normalizeBase(String basePath) {
        if (basePath.isEmpty() == false && basePath.endsWith("/") == false) {
            return basePath + "/";
        }
        return basePath;
    }
}
