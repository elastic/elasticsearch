/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.cache.Lucene90CompoundEntriesReader;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Prototype ("cheap shards", phase 5, item 3, default off, see
 * {@link FastSearchActivationSettings#FAST_ACTIVATION_PREFETCH_QUERY_RANGES_MAX_SIZE_SETTING}): right after a search shard activated from
 * a commit pointer has written the pointer's open-time bytes in the cache, prefetches (asynchronously, through the directory, so the
 * activation does not wait) the files that a first query reads with <em>dependent</em> reads whose byte ranges are only known once an
 * earlier read returns: terms index ({@code .tip}) and dictionary ({@code .tim}), stored fields index ({@code .fdx}, read synchronously by
 * {@code StoredFields#prefetch}), points index ({@code .kdi}, read node by node by numeric sort skipping) and vector graphs
 * ({@code .vex}). Their file ranges are known from the commit's file map (and, inside compound files, from the {@code .cfe}, which the
 * pointer carries), so they can all be requested in one wave before the query arrives.
 * <p>
 * Files are taken in that order of extension, smallest first within an extension, while the total stays within the bound; a file that
 * does not fit is skipped (a smaller one later may still fit).
 */
public final class ActivationQueryRangesPrefetcher {

    private static final Logger logger = LogManager.getLogger(ActivationQueryRangesPrefetcher.class);

    /** Extensions prefetched, in order of priority. */
    static final List<LuceneFilesExtensions> EXTENSIONS = List.of(
        LuceneFilesExtensions.TIP,
        LuceneFilesExtensions.FDX,
        LuceneFilesExtensions.KDI,
        LuceneFilesExtensions.TIM,
        LuceneFilesExtensions.VEX
    );

    private ActivationQueryRangesPrefetcher() {}

    /**
     * A range of {@code fileName} (a top-level commit file, possibly a compound file) to prefetch: the whole of a file of extension
     * {@code extension}, which is {@code fileName} itself or an entry of the compound file.
     */
    record Candidate(String fileName, LuceneFilesExtensions extension, long offset, long length) {}

    /**
     * The files to prefetch, in the order they are taken, within {@code maxBytes} in total.
     *
     * @param commitFiles the commit's files and their blob locations
     * @param cfeEntries  reads the entries of a {@code .cfe} file of the commit
     */
    static List<Candidate> select(Map<String, BlobLocation> commitFiles, CfeReader cfeEntries, long maxBytes) throws IOException {
        final List<Candidate> candidates = new ArrayList<>();
        for (var entry : commitFiles.entrySet()) {
            final String fileName = entry.getKey();
            final LuceneFilesExtensions extension = LuceneFilesExtensions.fromFile(fileName);
            if (extension == LuceneFilesExtensions.CFS) {
                final String cfe = fileName.substring(0, fileName.length() - "cfs".length()) + "cfe";
                if (commitFiles.containsKey(cfe) == false) {
                    continue;
                }
                for (var cfsEntry : cfeEntries.read(cfe).entrySet()) {
                    final LuceneFilesExtensions entryExtension = LuceneFilesExtensions.fromFile(cfsEntry.getKey());
                    if (entryExtension != null && EXTENSIONS.contains(entryExtension)) {
                        candidates.add(new Candidate(fileName, entryExtension, cfsEntry.getValue().offset(), cfsEntry.getValue().length()));
                    }
                }
            } else if (extension != null && EXTENSIONS.contains(extension)) {
                candidates.add(new Candidate(fileName, extension, 0L, entry.getValue().fileLength()));
            }
        }
        candidates.sort(
            Comparator.comparingInt((Candidate c) -> EXTENSIONS.indexOf(c.extension()))
                .thenComparingLong(Candidate::length)
                .thenComparing(Candidate::fileName)
        );
        final List<Candidate> selected = new ArrayList<>();
        long total = 0L;
        for (Candidate candidate : candidates) {
            if (candidate.length() > 0 && total + candidate.length() <= maxBytes) {
                selected.add(candidate);
                total += candidate.length();
            }
        }
        return selected;
    }

    @FunctionalInterface
    interface CfeReader {
        Map<String, Lucene90CompoundEntriesReader.FileEntry> read(String cfeFileName) throws IOException;
    }

    /**
     * Selects the files (see {@link #select}) and asks {@code directory} to prefetch them. Returns the number of bytes requested.
     * Best effort: failures are logged at debug and stop the prefetch.
     */
    public static long prefetch(Directory directory, Map<String, BlobLocation> commitFiles, long maxBytes) {
        if (maxBytes <= 0) {
            return 0L;
        }
        long requested = 0L;
        try {
            final List<Candidate> selected = select(commitFiles, cfe -> {
                // the .cfe is in the open-time bytes from the pointer, so this reads from the cache
                try (IndexInput in = directory.openInput(cfe, IOContext.READONCE)) {
                    return Lucene90CompoundEntriesReader.readEntries(in);
                }
            }, maxBytes);
            final Map<String, List<Candidate>> byFile = new LinkedHashMap<>();
            for (Candidate candidate : selected) {
                byFile.computeIfAbsent(candidate.fileName(), k -> new ArrayList<>()).add(candidate);
            }
            for (var entry : byFile.entrySet()) {
                try (IndexInput in = directory.openInput(entry.getKey(), IOContext.DEFAULT)) {
                    for (Candidate candidate : entry.getValue()) {
                        // non-blocking: starts the cache fill from the object store unless the range is cached already
                        in.prefetch(candidate.offset(), candidate.length());
                        requested += candidate.length();
                    }
                }
            }
            logger.debug("prefetched {} bytes of {} query files ({} files with ranges)", requested, selected.size(), byFile.size());
        } catch (Exception e) {
            logger.debug("failed to prefetch first-query ranges", e);
        }
        return requested;
    }
}
