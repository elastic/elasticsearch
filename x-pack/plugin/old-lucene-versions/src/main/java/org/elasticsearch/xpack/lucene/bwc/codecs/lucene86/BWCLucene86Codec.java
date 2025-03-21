/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene86;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.backward_codecs.lucene86.Lucene86SegmentInfoFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;

/**
 * This is a fork of {@link org.apache.lucene.backward_codecs.lucene86.Lucene86Codec}
 * Implements the Lucene 8.6 index format. Loaded via SPI for indices created/written with Lucene 8.6.0-8.6.2
 * (Elasticsearch [7.9.0-7.9.3]), mounted as archive indices in Elasticsearch 8.x / 9.x.
 */
public class BWCLucene86Codec extends BWCCodec {

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
    public BWCLucene86Codec() {
        super("BWCLucene86Codec");
        this.storedFieldsFormat = new Lucene50StoredFieldsFormat(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
        this.defaultDVFormat = new Lucene80DocValuesFormat();
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
