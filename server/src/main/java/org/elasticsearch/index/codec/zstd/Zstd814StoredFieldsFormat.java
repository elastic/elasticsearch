/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * {@link org.apache.lucene.codecs.StoredFieldsFormat} that compresses blocks of data using ZStandard.
 *
 * Unlike Lucene's default stored fields format, this format does not make use of dictionaries (even though ZStandard has great support for
 * dictionaries!). This is mostly due to the fact that LZ4/DEFLATE have short sliding windows that they can use to find duplicate strings
 * (64kB and 32kB respectively). In contrast, ZSTD doesn't have such a limitation and can better take advantage of large compression
 * buffers.
 */
public final class Zstd814StoredFieldsFormat extends Lucene90CompressingStoredFieldsFormat {

    // ZSTD has special optimizations for inputs that are less than 16kB and less than 256kB. So subtract a bit of memory from 16kB and
    // 256kB to make our inputs unlikely to grow beyond 16kB for BEST_SPEED and 256kB for BEST_COMPRESSION.
    private static final int BEST_SPEED_BLOCK_SIZE = (16 - 2) * 1_024;
    private static final int BEST_COMPRESSION_BLOCK_SIZE = (256 - 16) * 1_024;

    /** Attribute key for compression mode. */
    public static final String MODE_KEY = Zstd814StoredFieldsFormat.class.getSimpleName() + ".mode";

    public enum Mode {
        BEST_SPEED(1, BEST_SPEED_BLOCK_SIZE, 128),
        BEST_COMPRESSION(3, BEST_COMPRESSION_BLOCK_SIZE, 2048);

        final int level, blockSizeInBytes, blockDocCount;
        final Zstd814StoredFieldsFormat format;

        Mode(int level, int blockSizeInBytes, int blockDocCount) {
            this.level = level;
            this.blockSizeInBytes = blockSizeInBytes;
            this.blockDocCount = blockDocCount;
            this.format = new Zstd814StoredFieldsFormat(this);
        }

        public Zstd814StoredFieldsFormat getFormat() {
            return format;
        }
    }

    private final Mode mode;

    private Zstd814StoredFieldsFormat(Mode mode) {
        super("ZstdStoredFields814", new ZstdCompressionMode(mode.level), mode.blockSizeInBytes, mode.blockDocCount, 10);
        this.mode = mode;
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        // Both modes are compatible, we only put an attribute for debug purposes.
        String previous = si.putAttribute(MODE_KEY, mode.name());
        if (previous != null && previous.equals(mode.name()) == false) {
            throw new IllegalStateException(
                "found existing value for " + MODE_KEY + " for segment: " + si.name + "old=" + previous + ", new=" + mode.name()
            );
        }
        return super.fieldsWriter(directory, si, context);
    }

    public Mode getMode() {
        return mode;
    }

}
