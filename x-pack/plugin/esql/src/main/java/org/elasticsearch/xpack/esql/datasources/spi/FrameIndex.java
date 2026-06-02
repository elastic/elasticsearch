/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;

/**
 * Index of independently decompressible frames within a compressed file.
 * Used by {@link IndexedDecompressionCodec} to enable random access to
 * compressed data without scanning the entire file.
 *
 * <p>Each frame entry describes a contiguous region of compressed data
 * that can be decompressed independently. The decompressed sizes enable
 * the split provider to create splits targeting a specific decompressed
 * data volume.
 */
public record FrameIndex(List<FrameEntry> frames) {

    public record FrameEntry(long compressedOffset, long compressedSize, long decompressedSize) {}
}
