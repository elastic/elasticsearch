/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Immutable per-column counter snapshot exposed by {@link ParquetReaderCounters#snapshot()} under
 * the {@code "columns"} key.
 * <p>
 * Wire and XContent shape mirrors {@code BlobStoreActionStats}: a record that implements
 * {@link Writeable} + {@link ToXContentObject} so it can ride through the operator-status
 * {@code Map<String, Object>} envelope without bespoke serialization at the carrier level.
 *
 * @param bytesCompressedRead   on-disk bytes read for this column (sum across pages)
 * @param bytesDecompressed     bytes after page decompression
 * @param decompressionNanos    cumulative time spent decompressing pages
 * @param decodeNanos           cumulative time spent decoding values from pages
 * @param pagesRead             number of data pages read
 * @param materialization       {@code "eager"} or {@code "late"} — which materialization path
 *                              this column actually went through during the read
 */
public record PerColumnStatus(
    long bytesCompressedRead,
    long bytesDecompressed,
    long decompressionNanos,
    long decodeNanos,
    long pagesRead,
    String materialization
) implements Writeable, ToXContentObject {

    public static final String MATERIALIZATION_EAGER = "eager";
    public static final String MATERIALIZATION_LATE = "late";

    public PerColumnStatus {
        assert bytesCompressedRead >= 0 && bytesDecompressed >= 0 && decompressionNanos >= 0 && decodeNanos >= 0 && pagesRead >= 0
            : "all per-column counters must be non-negative, got "
                + bytesCompressedRead
                + "/"
                + bytesDecompressed
                + "/"
                + decompressionNanos
                + "/"
                + decodeNanos
                + "/"
                + pagesRead;
    }

    public PerColumnStatus(StreamInput in) throws IOException {
        this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(bytesCompressedRead);
        out.writeVLong(bytesDecompressed);
        out.writeVLong(decompressionNanos);
        out.writeVLong(decodeNanos);
        out.writeVLong(pagesRead);
        out.writeOptionalString(materialization);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("bytes_compressed_read", bytesCompressedRead);
        builder.field("bytes_decompressed", bytesDecompressed);
        builder.field("decompression_nanos", decompressionNanos);
        builder.field("decode_nanos", decodeNanos);
        builder.field("pages_read", pagesRead);
        if (materialization != null) {
            builder.field("materialization", materialization);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Flat {@code Map<String, Object>} representation used as the wire-format payload for the
     * per-column entries inside {@code AsyncExternalSourceOperator.Status.formatReader}. The outer
     * carrier serializes through {@code StreamOutput.writeGenericMap}, which requires every leaf
     * value to be in the {@code WRITERS} registry — typed records are not, so the per-column
     * snapshot is flattened at emit time. Keys mirror {@link #toXContent}.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> m = new LinkedHashMap<>(6);
        m.put("bytes_compressed_read", bytesCompressedRead);
        m.put("bytes_decompressed", bytesDecompressed);
        m.put("decompression_nanos", decompressionNanos);
        m.put("decode_nanos", decodeNanos);
        m.put("pages_read", pagesRead);
        if (materialization != null) {
            m.put("materialization", materialization);
        }
        return Collections.unmodifiableMap(m);
    }
}
