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

import java.util.ArrayList;
import java.util.HashMap;
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
            FileList hive = tryHive(normalizedBase, raw);
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

    /**
     * Groups files by the relative directory they were listed under, in original listing order, and
     * keeps one directory string per group. {@link HiveFileList#path(int)} then replays each file's
     * exact listed key. Grouping on the directory string — not on typed partition values — is what
     * makes the round-trip faithful: value spelling, segment order, non-partition directories, and a
     * base path unrelated to the keys all survive, since none of them is re-derived from the parsed
     * partition metadata.
     */
    private static FileList tryHive(String normalizedBase, GenericFileList raw) {
        List<StorageEntry> files = raw.files();
        int count = files.size();

        Map<String, Short> dirIndex = new HashMap<>();
        List<String> dirs = new ArrayList<>();
        short[] fileGroups = new short[count];
        long[] sizes = new long[count];
        long[] mtimes = new long[count];
        String[] leafNames = new String[count];
        String sharedExt = null;
        boolean extChecked = false;

        for (int f = 0; f < count; f++) {
            StorageEntry entry = files.get(f);
            sizes[f] = entry.length();
            mtimes[f] = entry.lastModified().toEpochMilli();

            String fullPath = entry.path().toString();
            String relative = fullPath;
            if (normalizedBase.isEmpty() == false && fullPath.startsWith(normalizedBase)) {
                relative = fullPath.substring(normalizedBase.length());
            }
            int lastSlash = relative.lastIndexOf('/');
            String dir = lastSlash >= 0 ? relative.substring(0, lastSlash + 1) : "";
            String leaf = lastSlash >= 0 ? relative.substring(lastSlash + 1) : relative;

            Short idx = dirIndex.get(dir);
            if (idx == null) {
                if (dirs.size() >= 65535) {
                    return null;
                }
                idx = (short) dirs.size();
                dirIndex.put(dir, idx);
                dirs.add(dir);
            }
            fileGroups[f] = idx;

            String ext = extractExtension(leaf);
            if (extChecked == false) {
                sharedExt = ext;
                extChecked = true;
            } else if (sharedExt != null && (ext == null || sharedExt.equals(ext) == false)) {
                sharedExt = null;
            }
            leafNames[f] = leaf;
        }

        if (sharedExt != null) {
            for (int i = 0; i < count; i++) {
                if (leafNames[i].endsWith(sharedExt)) {
                    leafNames[i] = leafNames[i].substring(0, leafNames[i].length() - sharedExt.length());
                }
            }
        }

        int numGroups = dirs.size();
        long[] gMtimes = new long[numGroups];
        boolean[] groupSeen = new boolean[numGroups];
        boolean uniformMtimes = true;
        for (int f = 0; f < count && uniformMtimes; f++) {
            int g = Short.toUnsignedInt(fileGroups[f]);
            if (groupSeen[g] == false) {
                groupSeen[g] = true;
                gMtimes[g] = mtimes[f];
            } else if (gMtimes[g] != mtimes[f]) {
                uniformMtimes = false;
            }
        }

        return new HiveFileList(
            normalizedBase,
            dirs.toArray(new String[0]),
            fileGroups,
            sizes,
            uniformMtimes ? null : mtimes,
            uniformMtimes ? gMtimes : null,
            leafNames,
            sharedExt,
            raw.originalPattern(),
            raw.partitionMetadata(),
            count,
            raw.fileSetFingerprint()
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
            count,
            raw.fileSetFingerprint()
        );
    }

    private static String normalizeBase(String basePath) {
        if (basePath.isEmpty() == false && basePath.endsWith("/") == false) {
            return basePath + "/";
        }
        return basePath;
    }
}
