/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ColumnMapping;
import org.elasticsearch.xpack.esql.datasources.FileFingerprint;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A dataset's SCHEMA, held so that discovering the schema of the same dataset again, under the same settings, opens
 * no file. If nothing the schema depends on has changed, there is nothing to learn by reading.
 * <p>
 * Schema only, deliberately. A resolve also gathers each file's statistics, and those are neither schema nor small:
 * five numbers per column per file, which at twenty columns weigh about 10kb per file by this cache's own reckoning —
 * some 100mb for a 9,705-file dataset, against a slice of 1.6mb on an 8gb node. Holding them would put every large
 * dataset over the limit and win nothing at all. They stay where they are built: on the first read, into the per-file
 * cache.
 * <p>
 * What is held follows from what the key identifies. Keyed on the anchor, the entry holds what the anchor decides and
 * leaves the listing-dependent part to be recomputed, so a file appended after the anchor is served rather than
 * missed. Keyed on every file, the listing a hit is served against is the one the entry was computed from, so the
 * finished result is held whole and nothing is recomputed.
 * <p>
 * Holds plain data only — names and types rather than attributes — so every serve builds its attributes afresh and no
 * two queries share a {@code NameId}. The conversion to and from a resolved source lives with the resolver.
 */
public sealed interface DatasetSchema permits DatasetSchema.FromAnchor, DatasetSchema.FromEveryFile {

    /** This entry's weight against the cache budget. */
    long estimatedBytes();

    /**
     * {@code first_file_wins}. The anchor's metadata as it stands before the listing-dependent finishing step, which is
     * the whole of what the anchor decides. Partition columns, the file count and each file's mapping are recomputed
     * from the listing on every serve, which is purely CPU work, and each file's statistics are read where they are
     * read today — this resolve never gathered them.
     */
    record FromAnchor(SchemaCacheEntry anchor, List<String> readerNotices) implements DatasetSchema {
        public FromAnchor {
            readerNotices = List.copyOf(readerNotices);
        }

        @Override
        public long estimatedBytes() {
            return anchor.estimatedBytes() + noticesBytes(readerNotices);
        }
    }

    /**
     * {@code union_by_name} and {@code strict}. The finished result: the dataset's metadata, each distinct per-file
     * read schema once, and for every file which of those it reads at, how its columns map, and what was learned
     * about it.
     */
    record FromEveryFile(
        SchemaCacheEntry dataset,
        List<SchemaCacheEntry> fileSchemas,
        Map<FileFingerprint, FileShape> files,
        List<String> readerNotices
    ) implements DatasetSchema {
        public FromEveryFile {
            fileSchemas = List.copyOf(fileSchemas);
            files = Map.copyOf(files);
            readerNotices = List.copyOf(readerNotices);
        }

        /**
         * @param fileSchema the index into {@link #fileSchemas} of the schema this file is read at
         * @param mapping    how the file's columns map onto the dataset's, or {@code null} when they are the same
         */
        public record FileShape(int fileSchema, @Nullable ColumnMapping mapping, @Nullable Map<String, DataType> inferredTypes) {
            public FileShape {
                inferredTypes = inferredTypes == null ? null : Map.copyOf(inferredTypes);
            }

            long estimatedBytes() {
                return 8 + (inferredTypes == null ? 0 : inferredTypes.size() * 64L);
            }
        }

        @Override
        public long estimatedBytes() {
            long bytes = dataset.estimatedBytes();
            for (SchemaCacheEntry schema : fileSchemas) {
                bytes += schema.estimatedBytes();
            }
            // A mapping is shared by every file that maps the same way, so each distinct one is counted once. Its size is
            // an int and a type reference per dataset column.
            Set<ColumnMapping> distinctMappings = new HashSet<>();
            long perFile = 0;
            for (FileShape shape : files.values()) {
                perFile += shape.estimatedBytes();
                if (shape.mapping() != null) {
                    distinctMappings.add(shape.mapping());
                }
            }
            bytes += distinctMappings.size() * (64L + dataset.columnNames().length * 12L);
            return bytes + filesBytes(perFile, files.size()) + noticesBytes(readerNotices);
        }
    }

    /**
     * What each file's reader said while it was read — the per-path notice channel, which is not the one the
     * reconcile writes to. Held so a served resolve can say it again; a resolve that reads no file would otherwise
     * go quiet about it.
     */
    private static long noticesBytes(List<String> notices) {
        long bytes = 0;
        for (String notice : notices) {
            bytes += 48 + notice.length() * 2L;
        }
        return bytes;
    }

    /** A per-file map's own overhead: a 16-byte fingerprint key and a map entry per file, on top of its values. */
    private static long filesBytes(long values, int count) {
        return values + count * (16L + 48L);
    }
}
