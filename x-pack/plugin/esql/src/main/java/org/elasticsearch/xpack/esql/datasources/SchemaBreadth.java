/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;

/**
 * How much of a dataset its schema is defined over. This is the property that decides whether a schema can be
 * answered from part of the dataset, and it belongs to the dataset's configuration rather than to any query.
 */
public enum SchemaBreadth {

    /** A declared mapping: the schema is the declaration, so no file defines it. */
    DECLARATION {
        /** Nothing to cache: the schema is the declaration, and no file's identity decides it. */
        @Override
        public InferredFrom inferredFrom(FileList listing) {
            return null;
        }
    },

    /** {@code first_file_wins}: one file defines it, and which file is decided by listing order. */
    ONE_FILE {
        /**
         * The anchor alone, taken from the same listing the resolve reads it from, so the two cannot disagree about
         * which file it is. A file appended after the anchor leaves this unchanged; one arriving before it becomes
         * the new anchor and changes it. Listing order needs no separate place in the identity: it decides only
         * which file is the anchor, and the anchor's own fingerprint already records that.
         * <p>
         * Answerable from a truncated listing too, since a prefix still has a head.
         */
        @Override
        public InferredFrom inferredFrom(FileList listing) {
            return listing.fileCount() < 2 ? null : new InferredFrom.Anchor(listing.fileFingerprint(0));
        }
    },

    /**
     * {@code union_by_name} and {@code strict}: every file contributes, by contract. Answering from part of the
     * dataset would report a narrower schema than it has, which is the outcome these modes exist to prevent.
     */
    EVERY_FILE {
        /**
         * The whole set. {@link FileList#fileSetFingerprint()} is already null where no set identity can be trusted —
         * a single file, a truncated listing, a sentinel — which is exactly where this breadth cannot be answered.
         */
        @Override
        public InferredFrom inferredFrom(FileList listing) {
            FileSetFingerprint files = listing.fileSetFingerprint();
            return files == null ? null : new InferredFrom.EveryFile(files);
        }
    };

    /**
     * The identity of the files this breadth's schema is inferred from, or {@code null} when a cached schema for
     * {@code listing} cannot be keyed — nothing to cache, or not enough of the listing known to key it. Each breadth
     * answers for itself, so a key built from this never branches on the discovery mode.
     */
    @Nullable
    public abstract InferredFrom inferredFrom(FileList listing);

    public static SchemaBreadth of(FormatReader.SchemaResolution schemaResolution) {
        return switch (schemaResolution) {
            case FIRST_FILE_WINS -> ONE_FILE;
            case UNION_BY_NAME, STRICT -> EVERY_FILE;
        };
    }

    /** Whether a schema this wide can be answered from a prefix of the dataset at all. */
    public boolean answerableFromAPrefix() {
        return this != EVERY_FILE;
    }
}
