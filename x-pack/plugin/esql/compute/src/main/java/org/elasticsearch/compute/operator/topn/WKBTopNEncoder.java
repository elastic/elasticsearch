/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.nio.ByteOrder;

/**
 * Encodes WKB bytes[].
 * Since WKB can contain bytes with zero value, which are used as terminator bytes, we need to encode differently.
 * Our initial implementation is to re-write to WKT and encode with UTF8TopNEncoder.
 * This is likely very inefficient.
 * We cannot use the UTF8TopNEncoder as is, because it removes the continuation byte, which could be a valid value in WKB.
 */
final class WKBTopNEncoder extends SortableTopNEncoder {
    @Override
    public int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder) {
        Geometry geometry = WellKnownBinary.fromWKB(GeometryValidator.NOOP, false, value.bytes, value.offset, value.length);
        String wkt = WellKnownText.toWKT(geometry);
        return UTF8.encodeBytesRef(BytesRefs.toBytesRef(wkt), bytesRefBuilder);
    }

    @Override
    public BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch) {
        String wkt = BytesRefs.toString(UTF8.decodeBytesRef(bytes, scratch));
        try {
            Geometry geometry = WellKnownText.fromWKT(GeometryValidator.NOOP, false, wkt);
            byte[] wkb = WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN);
            scratch.bytes = wkb;
            scratch.offset = 0;
            scratch.length = wkb.length;
            return scratch;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "WKBTopNEncoder";
    }

    @Override
    public TopNEncoder toSortable() {
        return this;
    }

    @Override
    public TopNEncoder toUnsortable() {
        return this;
    }
}
