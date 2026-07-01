/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Encodes a {@code semantic} field's original input value(s) for its internal binary doc values store, and decodes them back when
 * {@code _source} is rebuilt or the field is retrieved.
 * <p>
 * A leading kind byte selects the variant, so new kinds can be added without breaking existing values. {@link #TEXT} is stored as
 * UTF-8. {@link #BASE64_ENCODED_BINARY} is binary data with a media type, supplied as an {@link InferenceString} whose value is a
 * base64 data URI ({@code data:<media-type>;base64,<data>}). Its {@link DataType} is stored, the header up to and including the comma
 * (which holds the media type) is kept verbatim, and the base64 data is stored <b>decoded</b> and regenerated only on read (needed to
 * embed the bytes in JSON, or any string-only format), keeping the stored bytes compact. Storing decoded bytes (rather than the base64
 * text) keeps the value ~33% smaller both on disk and, more importantly, in memory wherever it is materialized uncompressed (indexing
 * buffers, merges, block decompression).
 * <p>
 * Non-text {@link InferenceString}s are always base64 data URIs (as {@code ShardBulkInferenceActionFilter} and {@link InferenceString}
 * enforce), so the base64 invariant is enforced explicitly here. Base64 decoding is <b>strict</b>: a malformed payload is rejected at
 * index time rather than stored unparsed, since the value must always be regeneratable on read.
 */
final class SemanticOriginalValueEncoder {
    private static final byte TEXT = 0;
    private static final byte BASE64_ENCODED_BINARY = 1;

    private SemanticOriginalValueEncoder() {}

    /**
     * Encodes a single original input value (a {@link String}, boolean or number for text, or a non-text {@link InferenceString} for a
     * base64 binary input).
     */
    static BytesRef encode(Object value) {
        if (value instanceof InferenceString inferenceString) {
            // Text values must arrive as plain strings: ShardBulkInferenceActionFilter rejects InferenceStrings that represent text
            // (objects for text values are not supported), so a text InferenceString reaching the encoder is a bug.
            if (inferenceString.isText()) {
                throw new IllegalArgumentException(
                    "Text values must be supplied as a string, not as an InferenceString of type [" + inferenceString.dataType() + "]"
                );
            }
            return encodeBinary(inferenceString);
        }
        // A boolean or number is coerced to its string form (consistent with the inference filter's SemanticTextUtils#nodeStringValues).
        String text = value.toString();
        byte[] textBytes = text.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[1 + textBytes.length];
        out[0] = TEXT;
        System.arraycopy(textBytes, 0, out, 1, textBytes.length);
        return new BytesRef(out);
    }

    private static BytesRef encodeBinary(InferenceString value) {
        // Data URI: data:<media-type>;base64,<data> (the header is split at the comma, as InferenceString validates it). The data type
        // is stored, the header is kept verbatim (it holds the media type), and the base64 data is stored decoded to keep it compact.
        final String dataUri = value.value();
        final int dataStart = dataUri.indexOf(',') + 1;
        final String header = dataUri.substring(0, dataStart);
        final byte[] prefix = header.getBytes(StandardCharsets.UTF_8);
        // A non-text InferenceString is always a base64 data URI, so the header must declare base64. Enforce the invariant explicitly:
        // the stored form is always the decoded bytes, regenerated as base64 on read.
        if (isBase64(header) == false) {
            throw new IllegalArgumentException(
                "Expected a base64 data URI for the ["
                    + value.dataType()
                    + "] value but its header ["
                    + header
                    + "] does not declare [;base64,]"
            );
        }
        final byte[] data;
        try {
            // Strict: a malformed base64 payload is rejected here rather than stored unparsed (it must be regeneratable on read).
            // Java's basic decoder rejects whitespace, incorrect padding and illegal characters.
            data = Base64.getDecoder().decode(dataUri.substring(dataStart));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid base64 payload in the [" + value.dataType() + "] data URI value", e);
        }
        try (BytesStreamOutput out = new BytesStreamOutput(3 + prefix.length + data.length)) {
            out.writeByte(BASE64_ENCODED_BINARY);
            // The ordinal is persisted, so DataType must stay append-only (as for its writeEnum transport serialization).
            out.writeByte((byte) value.dataType().ordinal());
            out.writeVInt(prefix.length);
            out.writeBytes(prefix);
            out.writeBytes(data);
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** A data URI header carries base64 data when it ends with {@code ;base64,} (otherwise the data is percent-encoded text). */
    private static boolean isBase64(String dataUriHeader) {
        return dataUriHeader.endsWith(";base64,");
    }

    /**
     * Decodes a single encoded value and writes it back to {@code builder} in its original {@code _source} form: a string for text,
     * or a {@code {type, format, value}} object for a data URI input (with the base64 payload regenerated here).
     */
    static void decodeAndWrite(BytesRef encoded, XContentBuilder builder) throws IOException {
        // Write directly from bytes (no intermediate String) to keep heap usage low when reconstructing large values.
        final byte kind = encoded.bytes[encoded.offset];
        switch (kind) {
            case TEXT -> builder.utf8Value(encoded.bytes, encoded.offset + 1, encoded.length - 1);
            case BASE64_ENCODED_BINARY -> {
                final DecodedBinary binary = readBinary(encoded);
                builder.startObject();
                builder.field(InferenceString.TYPE_FIELD, binary.dataType());
                // Every non-text DataType defaults to (and only supports) base64, which is the format the encoder stores and regenerates;
                // the reconstructed object reports that default format. See DataType.
                builder.field(InferenceString.FORMAT_FIELD, binary.dataType().getDefaultFormat());
                builder.field(InferenceString.VALUE_FIELD);
                builder.utf8Value(binary.dataUri(), 0, binary.dataUri().length);
                builder.endObject();
            }
            default -> throw new IllegalStateException("Unknown semantic value encoding [" + kind + "]");
        }
    }

    /**
     * Decodes a single encoded value into its {@code _source} value form (the inverse of {@link #encode}): a {@link String} for
     * text, or a {@code {type, format, value}} map for a data URI input. Used by the doc-values value fetcher so retrieval (the
     * {@code fields} option, highlighting) can read from the binary store without rebuilding {@code _source}.
     */
    static Object decode(BytesRef encoded) throws IOException {
        final byte kind = encoded.bytes[encoded.offset];
        return switch (kind) {
            case TEXT -> new String(encoded.bytes, encoded.offset + 1, encoded.length - 1, StandardCharsets.UTF_8);
            case BASE64_ENCODED_BINARY -> {
                final DecodedBinary binary = readBinary(encoded);
                final Map<String, Object> object = new LinkedHashMap<>();
                object.put(InferenceString.TYPE_FIELD, binary.dataType().toString());
                object.put(InferenceString.FORMAT_FIELD, binary.dataType().getDefaultFormat().toString());
                object.put(InferenceString.VALUE_FIELD, new String(binary.dataUri(), StandardCharsets.UTF_8));
                yield object;
            }
            default -> throw new IllegalStateException("Unknown semantic value encoding [" + kind + "]");
        };
    }

    private record DecodedBinary(DataType dataType, byte[] dataUri) {}

    /** Reads a {@link #BASE64_ENCODED_BINARY} value, regenerating the data URI by re-encoding the decoded payload as base64. */
    private static DecodedBinary readBinary(BytesRef encoded) throws IOException {
        final ByteArrayStreamInput in = new ByteArrayStreamInput();
        in.reset(encoded.bytes, encoded.offset + 1, encoded.length - 1);
        final DataType dataType = DataType.values()[in.readByte()];
        final byte[] prefix = new byte[in.readVInt()];
        in.readBytes(prefix, 0, prefix.length);
        final int dataLength = in.available();
        final byte[] dataUri;
        if (isBase64(new String(prefix, StandardCharsets.UTF_8))) {
            // We store the decoded bytes and regenerate base64 on read, so the re-encoded payload is always canonical (padded);
            // a non-canonical input is normalized in the reconstructed _source. The decoded bytes are unchanged.
            // Reassemble [header][base64] in a single buffer. Base64.encode(src, dst) can only write at dst index 0, so encode into
            // the front of the buffer and shift the result right by the header length - this avoids allocating a second full-size
            // base64 array (the regenerated data URI is already ~33% larger than the stored bytes).
            final byte[] data = new byte[dataLength];
            in.readBytes(data, 0, dataLength);
            final int base64Length = 4 * ((dataLength + 2) / 3);
            dataUri = new byte[prefix.length + base64Length];
            final int written = Base64.getEncoder().encode(data, dataUri);
            assert written == base64Length : written + " != " + base64Length;
            System.arraycopy(dataUri, 0, dataUri, prefix.length, base64Length);
            System.arraycopy(prefix, 0, dataUri, 0, prefix.length);
        } else {
            dataUri = new byte[prefix.length + dataLength];
            System.arraycopy(prefix, 0, dataUri, 0, prefix.length);
            in.readBytes(dataUri, prefix.length, dataLength);
        }
        return new DecodedBinary(dataType, dataUri);
    }
}
