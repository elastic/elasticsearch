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
 * A dataset's resolution, held so that resolving the same dataset again, under the same settings, can be answered
 * without opening a file. If nothing the result depends on has changed, there is nothing to learn by reading.
 * <p>
 * What is held follows from what the key identifies. Keyed on the anchor, the entry holds what the anchor decides and
 * leaves the listing-dependent part to be recomputed, so a file appended after the anchor is served rather than
 * missed. Keyed on every file, the listing a hit is served against is the one the entry was computed from, so the
 * finished result is held whole and nothing is recomputed.
 * <p>
 * Holds plain data only — names and types rather than attributes — so every serve builds its attributes afresh and no
 * two queries share a {@code NameId}. The conversion to and from a resolved source lives with the resolver.
 */
public sealed interface DatasetResolution permits DatasetResolution.FromAnchor, DatasetResolution.FromEveryFile {

    /** This entry's weight against the cache budget. */
    long estimatedBytes();

    /**
     * What a resolve learned about one file that the schema alone does not say: its statistics, which split planning
     * uses to skip reopening it, and its own native types, which the statistics are normalized against.
     *
     * @param statistics    the file's statistics as the per-file cache embeds them; empty when it had none
     * @param inferredTypes the file's types before any retype, or {@code null} when its read schema is its own
     */
    record FileFacts(Map<String, Object> statistics, @Nullable Map<String, DataType> inferredTypes) {
        public FileFacts {
            statistics = Map.copyOf(statistics);
            inferredTypes = inferredTypes == null ? null : Map.copyOf(inferredTypes);
        }

        long estimatedBytes() {
            return 48 + statistics.size() * 100L + (inferredTypes == null ? 0 : inferredTypes.size() * 64L);
        }
    }

    /**
     * {@code first_file_wins}. The anchor's metadata as it stands before the listing-dependent finishing step, and
     * what was learned about each file. Partition columns, the file count and each file's mapping are recomputed from
     * the listing on every serve, which is purely CPU work.
     */
    record FromAnchor(SchemaCacheEntry anchor, Map<FileFingerprint, FileFacts> files) implements DatasetResolution {
        public FromAnchor {
            files = Map.copyOf(files);
        }

        @Override
        public long estimatedBytes() {
            return anchor.estimatedBytes() + filesBytes(files.values().stream().mapToLong(FileFacts::estimatedBytes).sum(), files.size());
        }
    }

    /**
     * {@code union_by_name} and {@code strict}. The finished result: the dataset's metadata, each distinct per-file
     * read schema once, and for every file which of those it reads at, how its columns map, and what was learned
     * about it.
     */
    record FromEveryFile(SchemaCacheEntry dataset, List<SchemaCacheEntry> fileSchemas, Map<FileFingerprint, FileShape> files)
        implements
            DatasetResolution {
        public FromEveryFile {
            fileSchemas = List.copyOf(fileSchemas);
            files = Map.copyOf(files);
        }

        /**
         * @param fileSchema the index into {@link #fileSchemas} of the schema this file is read at
         * @param mapping    how the file's columns map onto the dataset's, or {@code null} when they are the same
         */
        public record FileShape(int fileSchema, @Nullable ColumnMapping mapping, FileFacts facts) {}

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
                perFile += 8 + shape.facts().estimatedBytes();
                if (shape.mapping() != null) {
                    distinctMappings.add(shape.mapping());
                }
            }
            bytes += distinctMappings.size() * (64L + dataset.columnNames().length * 12L);
            return bytes + filesBytes(perFile, files.size());
        }
    }

    /** A per-file map's own overhead: a 16-byte fingerprint key and a map entry per file, on top of its values. */
    private static long filesBytes(long values, int count) {
        return values + count * (16L + 48L);
    }
}
