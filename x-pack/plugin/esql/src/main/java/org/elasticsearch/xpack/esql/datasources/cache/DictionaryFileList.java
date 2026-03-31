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
import java.util.List;
import java.util.Map;

/**
 * Segment-dictionary-encoded file listing implementing {@link FileList}.
 * Compresses path storage from ~700 bytes/file (StorageEntry) to ~52 bytes/file
 * by interning path segments into a shared dictionary and referencing them by index.
 */
public final class DictionaryFileList implements FileList {

    /** Store basePath exactly as the common prefix string (e.g. "s3://bucket/data/") */
    private final String basePath;

    private final String[] tokens; // dictionary: index → segment string
    private final short[] pathTokens; // flat array of token indices for all paths
    private final int[] pathStarts; // start index in pathTokens for each file
    private final long[] sizes; // per-file size
    private final long[] mtimesMillis; // per-file mtime
    @Nullable
    private final String sharedExtension; // common extension (e.g. ".parquet")
    @Nullable
    private final String originalPattern;
    @Nullable
    private final PartitionMetadata partitionMetadata;
    private final int fileCount;

    DictionaryFileList(
        String basePath,
        String[] tokens,
        short[] pathTokens,
        int[] pathStarts,
        long[] sizes,
        long[] mtimesMillis,
        @Nullable String sharedExtension,
        @Nullable String originalPattern,
        @Nullable PartitionMetadata partitionMetadata,
        int fileCount
    ) {
        this.basePath = basePath;
        this.tokens = tokens;
        this.pathTokens = pathTokens;
        this.pathStarts = pathStarts;
        this.sizes = sizes;
        this.mtimesMillis = mtimesMillis;
        this.sharedExtension = sharedExtension;
        this.originalPattern = originalPattern;
        this.partitionMetadata = partitionMetadata;
        this.fileCount = fileCount;
    }

    /**
     * Builds a DictionaryFileList from a raw GenericFileList. Returns the original GenericFileList
     * if the token dictionary overflows ({@code > 65,535} unique segments).
     */
    public static FileList from(String basePath, GenericFileList raw) {
        if (raw == null || raw.isResolved() == false || raw.fileCount() == 0) {
            return raw;
        }

        List<StorageEntry> files = raw.files();
        int count = files.size();
        long[] sizes = new long[count];
        long[] mtimes = new long[count];

        Map<String, Short> tokenMap = new HashMap<>();
        List<String> tokenList = new ArrayList<>();
        List<short[]> fileTokensList = new ArrayList<>(count);
        String sharedExt = null;
        boolean extensionChecked = false;

        String normalizedBase = basePath;
        if (normalizedBase.isEmpty() == false && normalizedBase.endsWith("/") == false) {
            normalizedBase = normalizedBase + "/";
        }

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
                String segment = segments[s];
                Short idx = tokenMap.get(segment);
                if (idx == null) {
                    if (tokenList.size() >= 65535) {
                        return raw;
                    }
                    idx = (short) tokenList.size();
                    tokenMap.put(segment, idx);
                    tokenList.add(segment);
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
                            return raw;
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

    /**
     * Returns the extension starting at the first {@code '.'} when the suffix length is at least 4
     * (e.g. {@code ".par"} or longer).
     */
    static String extractExtension(String leafSegment) {
        int dot = leafSegment.indexOf('.');
        if (dot >= 0 && (leafSegment.length() - dot) >= 4) {
            return leafSegment.substring(dot);
        }
        return null;
    }

    @Override
    public int fileCount() {
        return fileCount;
    }

    @Override
    public StoragePath path(int i) {
        StringBuilder sb = new StringBuilder(basePath);
        int start = pathStarts[i];
        int end = pathStarts[i + 1];
        for (int t = start; t < end; t++) {
            if (t > start) {
                sb.append('/');
            }
            sb.append(tokens[Short.toUnsignedInt(pathTokens[t])]);
        }
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
        // basePath String: object header (40B) + char data
        bytes += basePath.length() * (long) Character.BYTES;
        for (String token : tokens) {
            // per-String: ~40B object overhead + char data
            bytes += 40 + token.length() * (long) Character.BYTES;
        }
        bytes += pathTokens.length * (long) Short.BYTES;
        bytes += (long) pathStarts.length * Integer.BYTES;
        bytes += sizes.length * (long) Long.BYTES;
        bytes += mtimesMillis.length * (long) Long.BYTES;
        if (sharedExtension != null) {
            bytes += 40 + sharedExtension.length() * (long) Character.BYTES;
        }
        return bytes;
    }

    /** Package-private accessors for {@link HiveFileList}. */
    String basePath() {
        return basePath;
    }

    String[] tokens() {
        return tokens;
    }
}
