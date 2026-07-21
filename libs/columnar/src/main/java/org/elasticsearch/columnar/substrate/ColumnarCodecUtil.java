/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.ColumnarFormat;

import java.io.IOException;

/**
 * Thin wrappers over {@link CodecUtil} that stamp every ColumNAR file with the format name and
 * {@link ColumnarFormat#VERSION_CURRENT}. Centralising this keeps the codec header/footer identical
 * across the substrate's files and gives one place to check the on-disk version on read.
 */
public final class ColumnarCodecUtil {

    private ColumnarCodecUtil() {}

    /** Writes a ColumNAR index header for {@code name} into {@code out}. */
    public static void writeHeader(IndexOutput out, String name, byte[] segmentId, String segmentSuffix) throws IOException {
        CodecUtil.writeIndexHeader(out, name, ColumnarFormat.VERSION_CURRENT, segmentId, segmentSuffix);
    }

    /**
     * Checks a ColumNAR index header on {@code in} and returns the on-disk version, which callers
     * branch on. Accepts any version in {@code [VERSION_START, VERSION_CURRENT]}.
     */
    public static int checkHeader(IndexInput in, String name, byte[] segmentId, String segmentSuffix) throws IOException {
        return CodecUtil.checkIndexHeader(in, name, ColumnarFormat.VERSION_START, ColumnarFormat.VERSION_CURRENT, segmentId, segmentSuffix);
    }

    /** Writes the trailing checksum footer. */
    public static void writeFooter(IndexOutput out) throws IOException {
        CodecUtil.writeFooter(out);
    }

    /** Validates the trailing checksum footer of a checksum input (used for meta files). */
    public static void checkFooter(ChecksumIndexInput in) throws IOException {
        CodecUtil.checkFooter(in);
    }

    /** Reads {@code in} end to end and validates its footer checksum (used for data files). */
    public static void checksumEntireFile(IndexInput in) throws IOException {
        CodecUtil.checksumEntireFile(in);
    }
}
