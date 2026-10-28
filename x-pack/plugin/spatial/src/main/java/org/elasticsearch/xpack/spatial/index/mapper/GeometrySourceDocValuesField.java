/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.CustomDocValuesField;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * Doc-value field that stores a {@code shape}/{@code geo_shape} field's geometries verbatim (as WKB), in input order, so
 * that columnar index modes can reconstruct {@code _source} and read the geometry value directly from doc values.
 * <p>
 * This is intentionally separate from {@link org.elasticsearch.lucene.spatial.BinaryShapeDocValuesField} (the tessellated
 * triangle tree used for spatial relations and aggregations): the triangle tree merges all values into one shape and is
 * lossy with respect to the individual geometries, so it cannot serve them back. The triangle tree remains the
 * representation read by aggregations; this field backs {@code _source} reconstruction and the value block loader.
 */
public class GeometrySourceDocValuesField extends CustomDocValuesField {

    private static final String FIELD_NAME_SUFFIX = ".source_geometry";

    private final List<Geometry> geometries = new ArrayList<>();

    public GeometrySourceDocValuesField(String name) {
        super(name);
    }

    /** The internal doc-value field name carrying the source geometries for the given shape field. */
    public static String fieldName(String shapeFieldName) {
        return shapeFieldName + FIELD_NAME_SUFFIX;
    }

    public void add(Geometry geometry) {
        geometries.add(geometry);
    }

    @Override
    public BytesRef binaryValue() {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(geometries.size());
            for (Geometry geometry : geometries) {
                out.writeBytesRef(new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN)));
            }
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode geometry source doc values", e);
        }
    }

    /**
     * Decodes the geometries previously written by {@link #binaryValue()}, preserving input order.
     */
    public static List<Geometry> decode(BytesRef bytesRef) throws IOException {
        ByteArrayStreamInput in = new ByteArrayStreamInput();
        in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        int count = in.readVInt();
        List<Geometry> geometries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            BytesRef wkb = in.readBytesRef();
            geometries.add(WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, wkb.bytes, wkb.offset, wkb.length));
        }
        return geometries;
    }

    /**
     * Decodes the raw WKB of each geometry previously written by {@link #binaryValue()}, preserving input order. Used by
     * the value block loader, which emits WKB directly.
     */
    public static List<BytesRef> decodeWkb(BytesRef bytesRef) throws IOException {
        ByteArrayStreamInput in = new ByteArrayStreamInput();
        in.reset(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        int count = in.readVInt();
        List<BytesRef> wkbs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            wkbs.add(in.readBytesRef());
        }
        return wkbs;
    }
}
