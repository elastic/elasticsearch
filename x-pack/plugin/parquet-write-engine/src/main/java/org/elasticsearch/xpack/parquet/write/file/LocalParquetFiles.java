/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Parquet files that the engine has written to the store directory of a shard and has not uploaded yet. A file is named by the range
 * of sequence numbers of its rows, and a row has exactly one sequence number, so the range also gives the number of rows. The name does
 * not start with an underscore or with {@code segments}, so Lucene's file deleter leaves it alone.
 */
public final class LocalParquetFiles {

    public static final String TEMP_PREFIX = "pqtmp";
    private static final Pattern NAME = Pattern.compile("pqlocal_(\\d{19})_(\\d{19})\\.parquet");
    private static final Pattern MERGED_NAME = Pattern.compile("pqmerged_(\\d{19})_(\\d{19})\\.parquet");

    /** A complete local file. {@code toSeqNo - fromSeqNo + 1} is its number of rows. */
    public record LocalFile(String name, long fromSeqNo, long toSeqNo, long length) {
        public long rows() {
            return toSeqNo - fromSeqNo + 1;
        }
    }

    private LocalParquetFiles() {}

    public static String name(long fromSeqNo, long toSeqNo) {
        return String.format(Locale.ROOT, "pqlocal_%019d_%019d.parquet", fromSeqNo, toSeqNo);
    }

    /** The name of the file that a flush merges the local files of the range into, before it uploads it. */
    public static String mergedName(long fromSeqNo, long toSeqNo) {
        return String.format(Locale.ROOT, "pqmerged_%019d_%019d.parquet", fromSeqNo, toSeqNo);
    }

    public static final String COMPACTED_PREFIX = "pqcompacted_";

    /** The name of the file that a compaction merges uploaded files into, before it uploads it. It is never a part of a flush. */
    public static String compactedName(long fromSeqNo, long toSeqNo) {
        return String.format(Locale.ROOT, COMPACTED_PREFIX + "%019d_%019d.parquet", fromSeqNo, toSeqNo);
    }

    /** The merged files of flushes that are not finished, in the order of their sequence numbers. */
    public static List<LocalFile> listMerged(Directory directory) throws IOException {
        final List<LocalFile> files = new ArrayList<>();
        for (String name : directory.listAll()) {
            final Matcher matcher = MERGED_NAME.matcher(name);
            if (matcher.matches()) {
                files.add(
                    new LocalFile(name, Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)), directory.fileLength(name))
                );
            }
        }
        files.sort((a, b) -> Long.compare(a.fromSeqNo(), b.fromSeqNo()));
        return files;
    }

    /** The complete local files in the order of their sequence numbers. */
    public static List<LocalFile> list(Directory directory) throws IOException {
        final List<LocalFile> files = new ArrayList<>();
        for (String name : directory.listAll()) {
            final Matcher matcher = NAME.matcher(name);
            if (matcher.matches()) {
                files.add(
                    new LocalFile(name, Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2)), directory.fileLength(name))
                );
            }
        }
        files.sort((a, b) -> Long.compare(a.fromSeqNo(), b.fromSeqNo()));
        return files;
    }

    /** Deletes the files of writes that did not finish. Their rows were never acknowledged. */
    public static void deleteTemporaryFiles(Directory directory) throws IOException {
        for (String name : directory.listAll()) {
            // a compaction can be cut off at any step, and its output is only a copy of rows that the table still lists in the old files
            if (name.startsWith(TEMP_PREFIX + "_") || name.startsWith(COMPACTED_PREFIX)) {
                directory.deleteFile(name);
            }
        }
    }
}
