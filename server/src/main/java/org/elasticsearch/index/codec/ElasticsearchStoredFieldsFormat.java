/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * Stored fields format that chooses between implementations per segment. Writes use the mode this instance was built with and
 * record it under {@link #MODE_KEY}; reads take the mode from the segment, so a reader resolves the implementation without
 * knowing the settings the segment was written with.
 *
 * <p>Each implementation records its own compression level separately, so the mode here names only the implementation.
 */
public final class ElasticsearchStoredFieldsFormat extends StoredFieldsFormat {

    /** Segment attribute holding the {@link Mode} a segment was written with. */
    public static final String MODE_KEY = "es.stored_fields.mode";

    public enum Mode {
        /** Lucene's stored fields. */
        LUCENE,
        /** Zstandard at the best-compression level. */
        ZSTD_BEST_COMPRESSION
    }

    private final Mode mode;
    private final Mode legacyMode;
    private final StoredFieldsFormat luceneFormat;

    /**
     * @param mode                   the mode segments written through this instance use
     * @param legacyMode the mode to read a segment with when it records none, which is what the codec wrote before
     *                               {@link #MODE_KEY} existed and so differs per codec
     * @param luceneFormat           the implementation backing {@link Mode#LUCENE}, which carries the Lucene compression level
     */
    public ElasticsearchStoredFieldsFormat(Mode mode, Mode legacyMode, StoredFieldsFormat luceneFormat) {
        this.mode = Objects.requireNonNull(mode);
        this.legacyMode = Objects.requireNonNull(legacyMode);
        this.luceneFormat = Objects.requireNonNull(luceneFormat);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        final String previous = si.putAttribute(MODE_KEY, mode.name());
        if (previous != null && previous.equals(mode.name()) == false) {
            throw new IllegalStateException(
                "segment [" + si.name + "] records stored fields mode [" + previous + "], cannot also write it as [" + mode + "]"
            );
        }
        return formatFor(mode).fieldsWriter(directory, si, context);
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return formatFor(modeOf(si, legacyMode)).fieldsReader(directory, si, fn, context);
    }

    /** The mode {@code si} was written with, taking this instance's answer for a segment that records none. */
    Mode modeOf(SegmentInfo si) {
        return modeOf(si, legacyMode);
    }

    /** The mode {@code si} was written with, or {@code legacyMode} when the segment records none. */
    static Mode modeOf(SegmentInfo si, Mode legacyMode) {
        final String value = si.getAttribute(MODE_KEY);
        if (value == null) {
            return legacyMode;
        }
        try {
            return Mode.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("segment [" + si.name + "] records an unknown stored fields mode [" + value + "]", e);
        }
    }

    /** The implementation segments written through this instance use. */
    StoredFieldsFormat writeFormat() {
        return formatFor(mode);
    }

    private StoredFieldsFormat formatFor(Mode mode) {
        return switch (mode) {
            case LUCENE -> luceneFormat;
            case ZSTD_BEST_COMPRESSION -> Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION.getFormat();
        };
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(mode=" + mode + ", before=" + legacyMode + ")";
    }
}
