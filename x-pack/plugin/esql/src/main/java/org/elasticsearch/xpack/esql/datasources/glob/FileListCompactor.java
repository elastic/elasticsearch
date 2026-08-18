/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Compresses a {@link GenericFileList} into a compact representation. Builds a
 * {@link DirectoryGroupedFileList} (when partition metadata was detected) and a {@link DictionaryFileList},
 * verifies each one replays the listed keys exactly, and keeps whichever verified candidate weighs less —
 * falling back to the original list if neither fits.
 * <p>
 * The round-trip verification (see {@link #verified}) is the single point that keeps every compact
 * representation faithful, even for a layout no encoding anticipates.
 */
final class FileListCompactor {

    private static final Logger logger = LogManager.getLogger(FileListCompactor.class);

    private FileListCompactor() {}

    /**
     * Compacts a raw file list into the smallest faithful representation available, or returns the
     * original list when compaction does not apply or overflows.
     * <p>
     * The directory-grouped encoding is only built when {@link PartitionMetadata} was detected — not
     * because it needs the partition values (it does not read them), but as a cost heuristic: that is
     * the signal a layout has repeated directories worth grouping. {@link GlobExpander} attaches no
     * partition metadata when hive partitioning is off, so such listings take the dictionary encoding
     * directly.
     */
    static FileList compact(String basePath, GenericFileList raw) {
        if (raw == null || raw.isResolved() == false || raw.fileCount() == 0) {
            return raw;
        }
        String normalizedBase = normalizeBase(basePath);
        PartitionMetadata pm = raw.partitionMetadata();
        FileList groupedCandidate = pm != null && pm.isEmpty() == false ? tryDirectoryGrouped(normalizedBase, raw) : null;
        FileList dictCandidate = tryDictionary(normalizedBase, raw);
        // Collect the listed keys once so both candidates verify against one array instead of walking the raw
        // entries again per candidate. These are stored strings; the reconstruction cost sits on the candidate
        // side. Skipped when neither encoding was built, since then there is nothing to verify.
        String[] listedPaths = groupedCandidate != null || dictCandidate != null ? listedPaths(raw) : null;
        FileList grouped = verified(groupedCandidate, raw, listedPaths);
        FileList dict = verified(dictCandidate, raw, listedPaths);
        // The directory-grouped encoding stores one string per directory while the dictionary shares path
        // segments across files, so which is smaller depends on the layout; keep whichever weighs less.
        if (grouped != null && (dict == null || grouped.estimatedBytes() <= dict.estimatedBytes())) {
            return grouped;
        }
        if (dict != null) {
            return dict;
        }
        return raw;
    }

    /** Collects the listed keys once so several candidate encodings verify against a single walk of the raw entries. */
    private static String[] listedPaths(GenericFileList raw) {
        String[] paths = new String[raw.fileCount()];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = raw.path(i).toString();
        }
        return paths;
    }

    /**
     * Strips {@code normalizedBase} from a listed key when it is genuinely a prefix, leaving the relative
     * path both encodings reconstruct from. When the base is not a prefix — e.g. a comma-separated resource
     * whose pattern prefix is the whole comma string — the full key is returned unchanged; the encoding
     * then reconstructs a wrong key and is discarded by {@link #verified}. Owning this rule in one place
     * keeps both encodings' notion of "relative" identical.
     */
    private static String relativize(String normalizedBase, String fullPath) {
        if (normalizedBase.isEmpty() == false && fullPath.startsWith(normalizedBase)) {
            return fullPath.substring(normalizedBase.length());
        }
        return fullPath;
    }

    /**
     * Returns the candidate only if it reproduces every listed file exactly — path, size and modification
     * time; otherwise {@code null}, so the caller falls through to the next encoding or the raw list. This
     * is the chokepoint that keeps every compact representation faithful even for a layout no encoding
     * anticipates — e.g. a base path that is not a prefix of the listed keys, as a comma-separated resource
     * produces on the first_file_wins rail. Path is compared as a string; a candidate whose reconstructed
     * key does not even parse is treated as a mismatch rather than allowed to throw. Size and mtime are
     * checked too because an encoding may collapse them per group (see {@link DirectoryGroupedFileList}).
     * {@code listedPaths} carries the raw listing's keys, collected once so verifying several candidates walks
     * the raw entries once.
     */
    private static FileList verified(FileList candidate, GenericFileList raw, String[] listedPaths) {
        if (candidate == null) {
            return null;
        }
        for (int i = 0; i < raw.fileCount(); i++) {
            String expected = listedPaths[i];
            String actual;
            try {
                actual = candidate.path(i).toString();
            } catch (IllegalArgumentException e) {
                actual = null;
            }
            if (expected.equals(actual) == false
                || candidate.size(i) != raw.size(i)
                || candidate.lastModifiedMillis(i) != raw.lastModifiedMillis(i)) {
                logger.debug(
                    "discarding {} for pattern [{}]: listed file [{}] reconstructs as [{}]",
                    candidate.getClass().getSimpleName(),
                    raw.originalPattern(),
                    expected,
                    actual
                );
                return null;
            }
        }
        return candidate;
    }

    private static String extractExtension(String leafSegment) {
        int dot = leafSegment.indexOf('.');
        if (dot >= 0 && (leafSegment.length() - dot) >= 4) {
            return leafSegment.substring(dot);
        }
        return null;
    }

    // ------------------------------------------------------------------
    // Directory-grouped encoding
    // ------------------------------------------------------------------

    /**
     * Groups files by the relative directory they were listed under, in original listing order, keeping
     * one directory string per group. {@link DirectoryGroupedFileList#path(int)} then replays each file's
     * exact listed key. Grouping on the directory string — not on typed partition values — is what makes
     * the round-trip faithful: value spelling, segment order and non-partition directories all survive,
     * since none of them is re-derived. Returns {@code null} when the directory count overflows the group
     * index.
     */
    private static FileList tryDirectoryGrouped(String normalizedBase, GenericFileList raw) {
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

            String relative = relativize(normalizedBase, entry.path().toString());
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

        return new DirectoryGroupedFileList(
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

            String relative = relativize(normalizedBase, entry.path().toString());

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
                String relative = relativize(normalizedBase, entry.path().toString());

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
