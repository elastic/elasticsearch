/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene87;

import org.apache.lucene.backward_codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.elasticsearch.index.codec.lucene50.Lucene50CompoundFormat;
import org.elasticsearch.index.codec.lucene50.Lucene50LiveDocsFormat;
import org.elasticsearch.index.codec.lucene60.Lucene60FieldInfosFormat;
import org.elasticsearch.index.codec.lucene86.Lucene86SegmentInfoFormat;
import org.elasticsearch.index.codec.lucene87.Lucene87StoredFieldsFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene86.Lucene86MetadataOnlyPointsFormat;

/**
 * Archive wrapper for Lucene 8.7 segments (Elasticsearch 7.10.0–7.17.x).
 *
 * <p>Used only when an Elasticsearch 6.x archive index was later written in 7.x, so some segments
 * still name {@code Lucene87Codec}. {@link BWCCodec} remaps that class to this wrapper. Indices
 * created in 7.x are opened read-only by the server {@code Lucene87} codec, not by this class.
 */
public class BWCLucene87Codec extends BWCCodec {

    private final LiveDocsFormat liveDocsFormat = new Lucene50LiveDocsFormat();
    private final CompoundFormat compoundFormat = new Lucene50CompoundFormat();
    private final PointsFormat pointsFormat = new Lucene86MetadataOnlyPointsFormat();
    private final DocValuesFormat defaultDVFormat;

    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return defaultDVFormat;
        }
    };

    private final StoredFieldsFormat storedFieldsFormat;

    // Needed for SPI loading
    @SuppressWarnings("unused")
    public BWCLucene87Codec() {
        super("BWCLucene87Codec");
        this.storedFieldsFormat = new Lucene87StoredFieldsFormat(Lucene87StoredFieldsFormat.Mode.BEST_COMPRESSION);
        this.defaultDVFormat = new Lucene80DocValuesFormat(Lucene80DocValuesFormat.Mode.BEST_COMPRESSION);
    }

    @Override
    protected FieldInfosFormat originalFieldInfosFormat() {
        return new Lucene60FieldInfosFormat();
    }

    @Override
    protected SegmentInfoFormat originalSegmentInfoFormat() {
        return new Lucene86SegmentInfoFormat();
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public PointsFormat pointsFormat() {
        return pointsFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }
}
