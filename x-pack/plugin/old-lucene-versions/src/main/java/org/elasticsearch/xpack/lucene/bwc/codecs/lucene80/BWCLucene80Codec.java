/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene80;

import org.apache.lucene.backward_codecs.lucene50.Lucene50CompoundFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50LiveDocsFormat;
import org.apache.lucene.backward_codecs.lucene50.Lucene50StoredFieldsFormat;
import org.apache.lucene.backward_codecs.lucene60.Lucene60FieldInfosFormat;
import org.apache.lucene.backward_codecs.lucene70.Lucene70SegmentInfoFormat;
import org.apache.lucene.backward_codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.elasticsearch.xpack.lucene.bwc.codecs.BWCCodec;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene60.Lucene60MetadataOnlyPointsFormat;

/**
 * Implements the Lucene 8.0 index format. Loaded via SPI for indices created/written with Lucene 8.0.0-8.3.0
 * (Elasticsearch [7.0.0-7.5.2]), mounted as archive indices in Elasticsearch 8.x / 9.x.
 */
public class BWCLucene80Codec extends BWCCodec {

    private final FieldInfosFormat fieldInfosFormat;
    private final SegmentInfoFormat segmentInfosFormat;
    private final StoredFieldsFormat storedFieldsFormat;
    private final LiveDocsFormat liveDocsFormat;
    private final CompoundFormat compoundFormat;
    private final DocValuesFormat docValuesFormat;
    private final PointsFormat pointsFormat;

    // Needed for SPI loading
    @SuppressWarnings("unused")
    public BWCLucene80Codec() {
        this("BWCLucene80Codec");
    }

    public BWCLucene80Codec(String name) {
        super(name);
        this.fieldInfosFormat = wrap(new Lucene60FieldInfosFormat());
        this.segmentInfosFormat = wrap(new Lucene70SegmentInfoFormat());
        this.storedFieldsFormat = new Lucene50StoredFieldsFormat(Lucene50StoredFieldsFormat.Mode.BEST_SPEED);
        this.liveDocsFormat = new Lucene50LiveDocsFormat();
        this.compoundFormat = new Lucene50CompoundFormat();
        this.docValuesFormat = new PerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return new Lucene80DocValuesFormat();
            }
        };
        this.pointsFormat = new Lucene60MetadataOnlyPointsFormat();
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @Override
    public final SegmentInfoFormat segmentInfoFormat() {
        return segmentInfosFormat;
    }

    @Override
    public final StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public final LiveDocsFormat liveDocsFormat() {
        return liveDocsFormat;
    }

    @Override
    public final CompoundFormat compoundFormat() {
        return compoundFormat;
    }

    @Override
    public final DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public final PointsFormat pointsFormat() {
        return pointsFormat;
    }
}
